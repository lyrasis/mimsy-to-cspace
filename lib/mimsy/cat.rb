# frozen_string_literal: true

# setup
#  - excludes loan rows as per https://3.basecamp.com/3410311/buckets/16953827/todos/2708290043
#  - if run in :test mode, limits to test records; otherwise makes working copy of catalogue
# build_relationships
#  - transforms working copy of catalogue into relationship info where relationship data
#    is present
#PRIVATE
# limit_to_test_records
#  - keep only records with mkeys listed in provided/co_select files
# make_working
#  - makes working copy of catalogue.csv
# exclude_loans
#  - remove loans not included in migration

module Mimsy
  module Cat
    extend self
    def setup
      case MODE
      when :full
        make_working
      when :test
        limit_to_test_records
      end
      exclude_loans
    end

    def build_relationships
      setup unless File.file?("#{DATADIR}/working/catalogue.tsv")
      relsjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @deduper = {}
        @srcrows = 0
        @outrows = 0

        @mkeylkup = {}
        @ids = []
        
        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/catalogue.tsv", csv_options: TSVOPT
        # Ruby's CSV gives us "CSV::Row" but we want Hash
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        # set up the job constants we will use to do matching within the table
        transform do |row|
          idnum = row.fetch(:id_number)
          @mkeylkup[row.fetch(:mkey)] = idnum 
          @ids << idnum
          row
        end

        # SECTION BELOW cleans up category1 values
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[category1],
          find: 'ACC+ESS+ION',
          replace: 'ACCESSION'

        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[category1],
          find: 'DETAL|DETAIl',
          replace: 'DETAIL'
        
        transform Merge::ConstantValueConditional,
          fieldmap: {
            objectName: 'Manuscript',
            objectNameLanguage: 'English'
          },
          conditions: {
            include: {
              field_equal: { fieldsets: [
                {
                  matches: [
                    ['row::category1', 'value::Manuscript']
                  ]
                }
              ]}
            }
          }
        transform Delete::FieldValueMatchingRegexp,
          fields: [:category1],
          match: '^Manuscript$'
        transform Delete::FieldValueMatchingRegexp,
          fields: [:category1],
          match: '^Gelatin silver print$'

        transform Merge::ConstantValueConditional,
          fieldmap: {
            category1: 'ACCESSION DETAIL'
          },
          conditions: {
            include: {
              field_equal: { fieldsets: [
                {
                  matches: [
                    ['row::category1', 'revalue::\d{2}-\d{3}']
                  ]
                }
              ]}
            }
          }
        # END SECTION

        transform Delete::FieldsExcept, keepfields: %i[id_number parent_key broader_text whole_part mkey category1]

        # SECTION BELOW populates :parent and rel_confidence columns

        # unambiguous relationship based on parent_key value match
        transform do |row|
          pk = row.fetch(:parent_key, nil)
          if pk
            if @mkeylkup.has_key?(pk)
              row[:parent] = @mkeylkup[pk]
              row[:rel_confidence] = 'high - based on unambiguous parent_key data relationship'
            else
              row[:parent] = nil
              row[:rel_confidence] = "zero - record with parent_key value (#{pk}) not found in data set"
            end
          else
            row[:parent] = nil
            row[:rel_confidence] = nil
          end
          row
        end

        # relationship based on whole_part value equalling id_number of another row
        #   other row is the parent
        #   cannot do this on ACCESSION rows because they often have their child id_number in this field
        #     and we don't want to set the child as the parent!
        transform do |row|
          if row.fetch(:parent, nil).nil? && row.fetch(:category1, '') == 'ACCESSION DETAIL'
            wp = row.fetch(:whole_part, nil)
            if wp == row.fetch(:id_number)
              row[:rel_confidence] = 'zero - ACCESSION DETAIL record, whole_part matches own id_number'
            elsif wp
              if @ids.any?(wp)
                row[:parent] = wp
                row[:rel_confidence] = 'medium-high - ACCESSION DETAIL record, whole_part field value exactly matches another row id_number'
              end
            end
          end
          row
        end

        # relationship based on splitting id_number on spaces, and trying to find an id that matches
        #   successively shorter segments of the id_number value
        transform do |row|
          if row.fetch(:parent, nil).nil? && row.fetch(:rel_confidence, nil).nil? && row.fetch(:category1, '') == 'ACCESSION DETAIL'
            id = row.fetch(:id_number, nil)
            ids = id.split(' ')
            x = ids.size - 1
            if x > 0
              x.times do |e|
                ids.pop
                compare = ids.join(' ')
                if @ids.any?(compare)
                  row[:parent] = compare
                  row[:rel_confidence] = 'medium - ACCESSION DETAIL record, id_number split on space matches another row id_number'
                  next
                end
              end
            end
          end
          row
        end

        # relationship based on splitting id_number on period, and trying to find an id that matches
        #   successively shorter segments of the id_number value
        transform do |row|
          if row.fetch(:parent, nil).nil? && row.fetch(:rel_confidence, nil).nil? && row.fetch(:category1, '') == 'ACCESSION DETAIL'
            id = row.fetch(:id_number, nil)
            ids = id.split('.')
            x = ids.size - 1
            if x > 0
              x.times do |e|
                ids.pop
                compare = ids.join('.')
                if @ids.any?(compare)
                  row[:parent] = compare
                  row[:rel_confidence] = 'medium - ACCESSION DETAIL record, id_number split on period matches another row id_number'            
                  next
                end
              end
            end
          end
          row
        end

        transform do |row|
          if row.fetch(:parent, nil).nil? && row.fetch(:rel_confidence, nil).nil? && row.fetch(:category1, '') == 'ACCESSION DETAIL'
            if row.fetch(:whole_part, nil)
              row[:rel_confidence] = 'zero - ACCESSION DETAIL record has whole/part value, but cannot find or match parent'
            else
              row[:rel_confidence] = 'zero - ACCESSION DETAIL record, no whole/part value, cannot find or match parent'
            end
          end
          row
        end

        transform do |row|
          if row.fetch(:parent, nil).nil? && row.fetch(:rel_confidence, nil).nil? && row.fetch(:category1, '') == 'ACCESSION'
            if row.fetch(:whole_part, nil)
              row[:rel_confidence] = 'zero - ACCESSION record has whole/part value, but cannot set parent relationship from parent'
            else
              row[:rel_confidence] = 'zero - ACCESSION record, no whole/part value, cannot find or match parent'
            end
          end
          row
        end

        transform do |row|
          if row.fetch(:parent, nil).nil? && row.fetch(:rel_confidence, nil).nil?
            if row.fetch(:whole_part, nil)
              row[:rel_confidence] = 'zero - unknown record level has whole/part value'
            else
              row[:rel_confidence] = 'zero - unknown record level, no whole/part value'
            end
          end
          row
        end
        # END SECTION
        transform Rename::Field, from: :parent, to: :parent_id
        #  show_me!

        
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/rels_co-co_data.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          initial_headers: %i[id_number parent_id rel_confidence parent_key broader_text whole_part],
          csv_options: TSVOPT
        
        post_process do
          puts "\n\nCOLLECTIONOBJECT RELATIONSHIP BUILDER"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(relsjob)
    end
    
    private_class_method def make_working
      @working = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/mimsy/catalogue.tsv",
          csv_options: TSVOPT
        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }
        
        #show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/catalogue_w.tsv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
        post_process do
          label = 'prelim_cat/make_working'
          puts "\n\n#{label.upcase}"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(@working)
    end

    private_class_method def limit_to_test_records
      @testrecs = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        @inittest = Lookup.csv_to_multi_hash(file: "#{DATADIR}/provided/co_select_init.tsv",
                                             csvopt: TSVOPT,
                                             keycolumn: :mkey)
        @loctest = Lookup.csv_to_multi_hash(file: "#{DATADIR}/provided/co_select_locs.tsv",
                                             csvopt: TSVOPT,
                                             keycolumn: :mkey)
        @reltest = Lookup.csv_to_multi_hash(file: "#{DATADIR}/provided/co_select_rels.tsv",
                                            csvopt: TSVOPT,
                                            keycolumn: :mkey)

        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/mimsy/catalogue.tsv",
          csv_options: TSVOPT
        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        # SECTION BELOW selects only listed rows for testing
        transform Merge::MultiRowLookup,
          lookup: @inittest,
          keycolumn: :mkey,
          fieldmap: {
            :keepinit => :mkey
          }
        transform Merge::MultiRowLookup,
          lookup: @loctest,
          keycolumn: :mkey,
          fieldmap: {
            :keeploc => :mkey
          }	
        transform Merge::MultiRowLookup,
          lookup: @reltest,
          keycolumn: :mkey,
          fieldmap: {
            :keeprel => :mkey
          }

        transform do |row|
          keepfields = row.keys.select{ |k| k.to_s.start_with?('keep') }
          val = []
          keepfields.each do |kf|
            val << row.fetch(kf, nil)
            row.delete(kf)
          end
          val = val.compact
          row[:keep] = val.size > 0 ? val.join(' ') : nil
          row
        end
        

        transform FilterRows::FieldPopulated, action: :keep, field: :keep
        transform Delete::Fields, fields: %i[keep]
        # # END
        
        #show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/catalogue_w.tsv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
        post_process do
          label = 'prelim_cat/limit_to_test_records'
          puts "\n\n#{label.upcase}"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(@testrecs)
    end

    private_class_method def exclude_loans
      @remove_loans = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/working/catalogue_w.tsv",
          csv_options: TSVOPT

        transform{ |r| r.to_h }

        transform{ |r| @srcrows += 1; r }

        transform CombineValues::FromFieldsWithDelimiter,
          sources: [:id_number, :category1],
          target: :id_number_cat,
          sep: ' ',
          delete_sources: false

        transform FilterRows::FieldMatchRegexp,
          action: :reject,
          field: :id_number_cat,
          match: '(BM-ZPB|L-CB).* Loan$'
        #show_me!
        
        transform{ |r| @outrows += 1; r }
        
        filename = "#{DATADIR}/working/catalogue.tsv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
        
        post_process do
          label = 'prelim_cat/exclude_loans'
          puts "\n\n#{label.upcase}"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(@remove_loans)
    end
  end
end
