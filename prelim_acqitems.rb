require_relative 'config'

module Mimsy
  module AcqItems
    def self.setup
      # PROCESSES
      # flagjob
      #   - flags rows having duplicate ID numbers if at least one row has an m_id (all rows marked y)
      #   - flags rows that have duplicate ID numbers
      # onecatreport
      #   Produces report of rows in acquisition_items where there is more than one row with the
      #     same id_number, and some are flagged as uncatalogued

      
      @flagjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @deduper = {}
        @srcrows = 0
        @outrows = 0

        @acqitems = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/acquisition_items.tsv",
                                             csvopt: TSVOPT,
                                             keycolumn: :id_number)

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/acquisition_items.tsv", csv_options: TSVOPT
        # Ruby's CSV gives us "CSV::Row" but we want Hash
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        #transform FilterRows::FieldEqualTo, action: :keep, field: :id_number, value: 'AGR 38.003'
        # SECTION BELOW flags all multiple rows with same id_number if any row in set has an m_id value
        transform Merge::MultiRowLookup,
          lookup: @acqitems,
          keycolumn: :id_number,
          fieldmap: {
            :mkeys => :m_id,
          },
          delim: MVDELIM
        transform Merge::CountOfMatchingRows,
          lookup: @acqitems,
          keycolumn: :id_number,
          targetfield: :ct
        transform do |row|
          mk = row.fetch(:mkeys, nil)
          if mk.blank?
            row[:mk_ct] = 0
          else
            mk = mk.split(MVDELIM, -1).reject{ |e| e.blank? }
            row[:mk_ct] = mk.size
          end
          row
        end
        transform do |row|
          mk = row.fetch(:mkeys, nil)
          ct = row.fetch(:mk_ct, 0)
          if mk.blank?
            row[:one_cat] = 'n'
          else
            if row.fetch(:ct) == 1
              row[:one_cat] = 'n'
            elsif row.fetch(:ct) == ct
              row[:one_cat] = 'n'
            else
              row[:one_cat] = mk.empty? ? 'n' : 'y'
            end
          end
          row
        end
        transform Delete::Fields, fields: %i[ct]
        # END SECTION
        
        transform Deduplicate::Flag, on_field: :id_number, in_field: :duplicate, using: @deduper
	
        #show_me!
        
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/acqitem_duplicates_flagged.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          initial_headers: %i[id_number],
          csv_options: TSVOPT
        
        post_process do
          puts "\n\nACQ ITEMS WITH DUPLICATES FLAGGED"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(@flagjob)

      @onecatreport = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/acqitem_duplicates_flagged.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldEqualTo, action: :keep, field: :one_cat, value: 'y'

        #show_me!
        
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/reports/acquitems_cat_and_uncat.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          initial_headers: %i[id_number],
          csv_options: TSVOPT
        
        post_process do
          puts "\n\nREPORT: ACQITEMS BOTH CAT AND UNCAT"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(@onecatreport)

      @dupeacqcatjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/acqitem_duplicates_flagged.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        # uncataloged only
        transform FilterRows::FieldPopulated, action: :reject, field: :m_id
        transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate, value: 'y'

        transform Delete::Fields, fields: %i[duplicate]

        #  show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/reports/DUPLICATE_acqitems_mapped_as_objects.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        post_process do
          puts "\n\nDUPLICATE OBJECT RECORDS FROM ACQITEMS"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(@dupeacqcatjob)

      @acqitemcatjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @deduper = {}
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/acqitem_duplicates_flagged.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldEqualTo, action: :reject, field: :one_cat, value: 'y'
        transform FilterRows::FieldEqualTo, action: :reject, field: :duplicate, value: 'y'
        # uncataloged only
        transform FilterRows::FieldPopulated, action: :reject, field: :m_id
        # id_number is required
        transform FilterRows::FieldPopulated, action: :keep, field: :id_number


        transform Rename::Field, from: :id_number, to: :objectNumber
        transform Rename::Field, from: :transfer_date, to: :assocstructureddategroup
        transform Merge::ConstantValueConditional,
          fieldmap: { assocdatetype: 'acquisition transfer date' },
          conditions: {
            exclude: {
              field_empty: { fieldsets: [
                {
                  fields: ['row::assocstructureddategroup']
                }
              ]
                           }
            }
          }
        transform Merge::ConstantValue, target: :inventoryStatus, value: 'not cataloged'


        transform do |row|
          summary = row.fetch(:item_summary, nil)
          if summary
            row[:briefDescription] = summary.split('LINEBREAKWASHERE').join(' -- ')
          else
            row[:briefDescription] = nil
          end
          row
        end

        # SECTION below adds columns used in collectionobject csv
        # It is unnecessary if we can't combine two data sources
        transform Merge::ConstantValue, target: :datasource, value: 'acqitem'
        # END SECTION

        transform Delete::FieldsExcept, keepfields: %i[objectNumber assocstructureddategroup assocdatetype
                                                       inventoryStatus briefDescription]

        #show_me!
        
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/acqitem_collectionobjects.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          initial_headers: %i[objectNumber],
          csv_options: TSVOPT
        
        post_process do
          puts "\n\nUNCAT OBJECT RECORDS FROM ACQ ITEMS"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(@acqitemcatjob)
    end
  end
end
