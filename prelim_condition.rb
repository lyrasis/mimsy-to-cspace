require_relative 'config'

# filter_records
#   only keep condition rows linked to objects we are migrating

module Mimsy
  module Condition
    extend self

    PRIORITY = {
      '0' => nil,
      '' => nil,
      nil => nil,
      '1' => 'high'
    }
    
    def filter_records
      Mimsy::Cat.setup

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

    def cs_condition
      #filter_records

      transform = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0
        
        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/working/condition.tsv",
          csv_options: TSVOPT
        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[objid condkey],
          target: :conditioncheckrefnumber,
          sep: '.'

        transform Rename::Field, from: :condition_date, to: :conditioncheckassessmentdate
        transform Rename::Field, from: :examined_by, to: :conditionchecker
        transform Rename::Field, from: :purpose, to: :conditioncheckreason
        transform Rename::Field, from: :status_date, to: :nextconditioncheckdate

        transform Replace::FieldValueWithStaticMapping,
          source: :priority_flag1,
          target: :objectauditcategory,
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
          find: 'overall-; ',
          replace: 'Overall: ',
          casesensitive: false
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[condition],
          find: '(stucture|Strucutre|structrue):',
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

#        transform FilterRows::FieldEqualTo, action: :keep, field: :conditioncheckrefnumber, value: '87-018.028.1522'
        
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
#              binding.pry
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

        # anything that appears before overall: -- map to conditionnotelhmc
        # anything in parens -- map to conditionnotelhmc
        
        transform Delete::Fields, fields: %i[m_id condition current_record priority_flag2]

#        show_me!
        #        transform FilterRows::FieldPopulated, action: :keep, field: :cp

        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/cs_condition.tsv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT,
          initial_headers: %i[conditioncheckrefnumber overall surface structure nofacet cp origcond]
        post_process do
          label = 'cs condtion records (working)'
          puts "\n\n#{label.upcase}"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(transform)
    end


  end
end
