# frozen_string_literal: true

# filter_records
#   only keep condition rows linked to objects we are migrating
# condition_prep
#   produce CS conditioncheck pre-records. Used to create relationships. Some fields need
#     to be deleted before these are real CS records
# csv
#   produce CS conditioncheck records
# relationship_to_object
#   produce CS object <-> condition check relationships

module Cspace
  module Condition
    extend self

    PRIORITY = {
      '0' => 'low',
      '' => nil,
      nil => nil,
      '1' => 'high'
    }
    
    def filter_records
      Mimsy::Cat.setup unless File.file?("#{DATADIR}/working/catalogue.tsv")

      limit = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        @cat = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/catalogue.tsv",
                                        csvopt: TSVOPT,
                                        keycolumn: :mkey)

        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/mimsy/condition.tsv",
          csv_options: TSVOPT
        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        # SECTION BELOW selects only listed rows for testing
        transform Merge::MultiRowLookup,
          lookup: @cat,
          keycolumn: :m_id,
          fieldmap: {
            :objid => :id_number
          }
        transform FilterRows::FieldPopulated, action: :keep, field: :objid
        # # END
        
        #show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/condition.tsv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
        post_process do
          label = 'condition data to process'
          puts "\n\n#{label.upcase}"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(limit)
    end

    def cs_condition_prep
      filter_records unless File.file?("#{DATADIR}/working/condition.tsv")

      transform = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0
        @deduper = {}
        @counter = 1
        
        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/working/condition.tsv",
          csv_options: TSVOPT
        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        # SECTION BELOW prepares conditioncheckrefnumber
        #  prepends CC, appends conditiondate if present
        #  flags any duplicates, adds integer to differentiate
        transform Copy::Field, from: :condition_date, to: :suffix
        
        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[objid suffix],
          target: :conditioncheckrefnumber,
          sep: '.',
          delete_sources: false
        transform Prepend::ToFieldValue, field: :conditioncheckrefnumber, value: 'CC.'
        transform Deduplicate::Flag, on_field: :conditioncheckrefnumber, in_field: :duplicate, using: @deduper
        transform do |row|
          duplicate = row[:duplicate]
          return row if duplicate.blank?

          if duplicate == 'y'
            ref_number = row[:conditioncheckrefnumber]
            row[:conditioncheckrefnumber] = "#{ref_number}.#{@counter}"
            @counter += 1
          end
          row
        end
        # END SECTION

        transform Rename::Field, from: :condition_date, to: :conditioncheckassessmentdate
        transform Rename::Field, from: :examined_by, to: :conditioncheckerperson
        transform Rename::Field, from: :purpose, to: :conditioncheckreason

        transform Replace::FieldValueWithStaticMapping,
          source: :priority_flag1,
          target: :conservationtreatmentpriority,
          mapping: PRIORITY,
          fallback_val: nil

        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[summary],
          find: 'LINEBREAKWASHERE',
          replace: "\n"

        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[summary note],
          target: :conditionchecknote,
          sep: "\n\n"

        # SECTION BELOW cleans and splits up condition field into different facets
        transform Copy::Field, from: :condition, to: :origcond
        
        transform do |row|
          cond = row.fetch(:condition, nil)
          if cond.blank?
            row[:cp] = nil
          else
            cond = cond.downcase
            if cond.match?(/: overall-/)
              row[:cp] = cond
              row[:condition] = nil
            elsif cond.match?(/\(/)
              row[:cp] = cond
              row[:condition] = nil
            elsif cond.match?(/^see .*(summ|below)/)
              row[:cp] = cond
              row[:condition] = nil
            elsif cond.match?(/^.$/)
              row[:cp] = nil
              row[:condition] = nil
            elsif cond.start_with?('lf-002')
              row[:cp] = cond
              row[:condition] = nil
            else
              row[:cp] = nil
            end
          end
          row
        end

        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[condition],
          find: ': a & b:? ',
          replace: ': a-b: ',
          casesensitive: false
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[condition],
          find: 'exzcellent|excellenty',
          replace: 'excellent',
          casesensitive: false
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[condition],
          find: 'excellent: stable',
          replace: 'excellent, stable',
          casesensitive: false
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[condition],
          find: 'overall-; ',
          replace: 'Overall: ',
          casesensitive: false
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[condition],
          find: 'Overall good',
          replace: 'Overall: good',
          casesensitive: false
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[condition],
          find: '(stucture|Strucutre|structrue|strucure):',
          replace: 'Structure: ',
          casesensitive: false
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[condition],
          find: '(surace|suface):',
          replace: 'Surface: ',
          casesensitive: false
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[condition],
          find: 'stalbe',
          replace: 'stable'
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[condition],
          find: ' -',
          replace: ': ',
          casesensitive: false
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[condition],
          find: ' *(Overall|Structure|Surface): *',
          replace: '|\1: ',
          casesensitive: false
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[condition],
          find: '([^|]) *(Overall|Surface|Structure)[:;,]',
          replace: '\1|\2:',
          casesensitive: false
        
        transform do |row|
          %i[overall surface structure nofacet].each{ |f| row[f] = nil }
          
          cond = row.fetch(:condition, nil)
          unless cond.blank? || cond.nil?
            if cond[': a-b:']
              row[:cp] = 'a-b'
              cond = cond.sub(': a-b:', ':')
            end
            
            condpipe = cond.split('|').map(&:strip).reject(&:empty?)
            if condpipe.size > 1
              other = []
              condarr = condpipe.map{ |c| c.split(':') }
              condarr.each do |c|
                if c.size == 2
                  type = c[0].downcase
                  if type.start_with?('overall')
                    row[:overall] = c[1]
                  elsif type.start_with?('surface')
                    row[:surface] = c[1]
                  elsif type.start_with?('structure')
                    row[:structure] = c[1]
                  else
                    other << c.join(':')
                  end
                else
                  other << c.join(':')
                end
              end
              row[:cp] = other.join('|')
              row[:condition] = nil
            else
              condarr = condpipe[0].split(' ')
              if condarr.size == 1
                row[:nofacet] = condarr.flatten[0]
                row[:condition] = nil
              elsif condarr.size == 2 && condarr[0].strip.match?(/[,;]$/)
                row[:nofacet] = condarr.join(' ').sub(';', ',')
                row[:condition] = nil
              elsif condarr[0].downcase.start_with?('overall') && condarr.size < 4
                condarr.delete_at(0)
                row[:overall] = condarr.join(' ')
                row[:condition] = nil
              else
                row[:cp] = cond.sub('|', '')
                row[:condition] = nil
              end
            end
          end
          row
        end

        transform Clean::DowncaseFieldValues, fields: %i[overall surface structure nofacet]
        transform Clean::StripFields, fields: %i[overall surface structure nofacet]
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[overall surface structure nofacet],
          find: ';',
          replace: ','
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[overall surface structure nofacet],
          find: '^[ ,.]+',
          replace: ''
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[overall surface structure nofacet],
          find: '[ ,.]+$',
          replace: ''
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[overall surface structure nofacet],
          find: '^(\w+) (\w+)$',
          replace: '\1, \2'
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[overall surface structure nofacet],
          find: '^(\w+),(\w+)$',
          replace: '\1, \2'
        # END SECTION

        # SECTION BELOW combines values into CS facet fields
        #        transform FilterRows::FieldEqualTo, action: :keep, field: :conditioncheckrefnumber, value: '94-029.01.10606'

        transform do |row|
          %i[overall surface structure nofacet].each do |f|
            # add all the fields we'll need so they exist in every row
            row["#{f}_cond".to_sym] = '%NULLVALUE%'
            row["#{f}_facet".to_sym] = '%NULLVALUE%'
            row["#{f}_date".to_sym] = '%NULLVALUE%'
            row["#{f}_note".to_sym] = '%NULLVALUE%'

            val = row.fetch(f, nil)
            next if val.blank?

            # populate *_condition
            row["#{f}_cond".to_sym] = val

            # populate *_facet
            case f
            when :overall
              facet = 'Overall'
            when :surface
              facet = 'Surface'
            when :structure
              facet = 'Structure'
            when :nofacet
              facet = '%NULLVALUE%'
            end
            row["#{f}_facet".to_sym] = facet

            # populate *_date
            d = row.fetch(:conditioncheckassessmentdate, nil)
            row["#{f}_date".to_sym] = d unless d.blank?

            # populate *_note
            n = row.fetch(:cp, nil)            
            row["#{f}_note".to_sym] = n unless n.blank?
          end

          # handle notes where we were not able to extract any structured facet data
          chk = %i[overall surface structure nofacet].map{ |f| row.fetch(f, nil) }.uniq
          if chk.size == 1 && chk[0].nil?
            v = row.fetch(:cp, nil)
            unless v.blank?
              row[:nofacet_note] = v
              d = row.fetch(:conditioncheckassessmentdate, nil)
              row[:nofacet_date] = d unless d.blank?
            end
          end
          row
        end

        # compile CS rows
        transform do |row|
          c = []
          f = []
          d = []
          n = []
          %i[overall surface structure nofacet].each do |facet|
            fields = %w[cond facet date note].map{ |f| "#{facet}_#{f}" }.map(&:to_sym)
            vals = fields.map{ |f| row.fetch(f, nil) }
            next if vals.uniq == ['%NULLVALUE%']
            c << vals[0]
            f << vals[1]
            d << vals[2]
            n << vals[3]
          end
          if c.size == 0
            row[:conditionlhmc] = nil
            row[:conditionfacetlhmc] = nil
            row[:conditiondatelhmc] = nil
            row[:conditionnotelhmc] = nil
          else
            row[:conditionlhmc] = c.join(';')
            row[:conditionfacetlhmc] = f.join(';')
            row[:conditiondatelhmc] = d.join(';')
            row[:conditionnotelhmc] = n.join(';')
          end
          row
        end

        # delete intermediate rows
        transform do |row|
          %i[overall surface structure nofacet].each do |facet|
            %w[cond facet date note].map{ |f| "#{facet}_#{f}" }.map(&:to_sym).each{ |f| row.delete(f) }
            row.delete(facet)
          end
          row.delete(:cp)
          row.delete(:origcond)
          row
        end
        # END SECTION

        # SECTION BELOW moves subsequent condition check reasons to field that can be joined into note
        transform do |row|
          ccr = row.fetch(:conditioncheckreason, nil)
          ccrs = ccr.split(';').map(&:strip) if ccr
          if !ccr.blank? && ccrs.size > 1
            row[:conditioncheckreason] = ccrs[0]
            row[:addlreason] = ccrs[1..-1].join('; ')
          else
            row[:addlreason] = nil	
          end

          ccr = row.fetch(:conditioncheckreason, nil)
          unless ccr.blank?
            ccrd = ccr.downcase
            if ccrd.match?(/^des?ins?tall/)
              row[:conditioncheckreason] = 'exhibition'
              row[:addlreason] = ccr
            elsif ccrd.start_with?('loan')
              row[:conditioncheckreason] = 'loanin'
              row[:addlreason] = ccr
            elsif ccrd == 'annual review'
              row[:conditioncheckreason] = 'annualreview'
            elsif ccrd == 'acquisition'
              row[:conditioncheckreason] = 'newacquisition'
            elsif ccrd == 'accession'
              row[:conditioncheckreason] = 'newacquisition'
            elsif ccrd == 'catalogue'
              row[:conditioncheckreason] = 'catalogue'
            elsif ccrd == 'hair brush'
              row[:conditioncheckreason] = nil
            end
          end
          row
        end

        transform Prepend::ToFieldValue, field: :addlreason, value: 'Additional reason: '

        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[addlreason conditionchecknote],
          target: :conditionchecknote,
          sep: "\n\n"

        # END SECTION
        
        # SECTION BELOW moves subsequent condition checkers to field that can be joined into note
        transform do |row|
          cc = row.fetch(:conditioncheckerperson, nil)
          cc = cc.split(';').map(&:strip) if cc
          if !cc.blank? && cc.size > 1
            row[:conditioncheckerperson] = cc[0]
            row[:addlchecker] = cc[1..-1].join('; ')
          else
            row[:addlchecker] = nil
          end
          row
        end

        transform Prepend::ToFieldValue, field: :addlchecker, value: 'Additional checker(s)/assessor(s): '
        
        # transform Clean::RegexpFindReplaceFieldVals,
        #   fields: %i[conditioncheckerperson],
        #   find: "\\\",
        #   replace: ''
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[conditioncheckerperson],
          find: ' \(conservator\)',
          replace: '',
          casesensitive: false
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[conditioncheckerperson],
          find: 'Book, Victoria',
          replace: 'Victoria Book',
          casesensitive: false

        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[addlchecker conditionchecknote],
          target: :conditionchecknote,
          sep: "\n\n"
        # END SECTION
        
        transform Delete::Fields, fields: %i[m_id condition current_record status_date priority_flag2 duplicate]

        #show_me!

        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/conditioncheck_pre.tsv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT,
          initial_headers: %i[conditioncheckrefnumber]
        post_process do
          label = 'pre-conditioncheck records'
          puts "\n\n#{label.upcase}"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(transform)
    end

    def csv
      cs_condition_prep unless File.file?("#{DATADIR}/working/conditioncheck_pre.tsv")

      transform = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0
        
        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/working/conditioncheck_pre.tsv",
          csv_options: TSVOPT
        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform Delete::Fields, fields: %i[condkey objid suffix]

        #show_me!

        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/cs/conditioncheck.csv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: LOCCSVOPT,
          initial_headers: %i[conditioncheckrefnumber]
        post_process do
          label = 'cs conditioncheck records'
          puts "\n\n#{label.upcase}"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(transform)
    end

    def relationship_to_object
      cs_condition_prep unless File.file?("#{DATADIR}/working/conditioncheck_pre.tsv")

      rels = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0
        
        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/working/conditioncheck_pre.tsv",
          csv_options: TSVOPT
        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[objid condkey],
          target: :objectIdentifier,
          sep: '.',
          delete_sources: false

        transform Rename::Field, from: :conditioncheckrefnumber, to: :objectIdentifier
        transform Merge::ConstantValue, target: :objectDocumentType, value: 'ConditionCheck'

        transform Rename::Field, from: :objid, to: :subjectIdentifier
        transform Merge::ConstantValue, target: :subjectDocumentType, value: 'CollectionObject'

        transform Delete::FieldsExcept, keepfields: %i[objectIdentifier objectDocumentType subjectIdentifier subjectDocumentType]

        #show_me!

        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/cs/rels_co-conditioncheck.csv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: LOCCSVOPT
        post_process do
          label = 'cs conditioncheck <-> object relationships'
          puts "\n\n#{label.upcase}"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(rels)
    end
  end
end
