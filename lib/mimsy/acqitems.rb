# frozen_string_literal: true

# flag_dupes
#   - flags rows having duplicate ID numbers if at least one row has an m_id (all rows marked y)
#   - flags rows that have duplicate ID numbers
# one_cat_report
#   Produces report of rows in acquisition_items where there is more than one row with the
#     same id_number, and some are flagged as uncatalogued
# duplicates
#  Produces report of duplicate acquisition_items which will not be migrated
# uncat_items
#  Produces table ready for mapping to collectionobject
# id_lookup
#  Purpose: use to create object-acquisition relationships (where objects are derived from acq_items table)
#  Keeps only rows with an id number but no MKEY linking to a catalogue row
#  outputs acquisition id, object id
# no_acquisition
#  Writes report of acquisition items not linked to any acquisitions
# one_acquisition
#  Writes informational report of acquisition items linked to one acquisition (as expected)
# multi_acquisition
#  Writes report of acquisition items linked to multiple acquisitions, which is not expected
# no_cat
#  Writes informational report of acquisition items not linked to any cataloguing record.
#  These "not catalogued" items are expected and will be turned into stub objects for migration
# one_cat
#  Writes informational report of acquisition items linked to one cataloguing record.
#  These catalogued items are expected and data from acq item will be merged into object record for migration
# multi_cat
#   Writes report of acquisition items linked to multiple cataloguing records.
#   Unexpected/problematic situation

module Mimsy
  module AcqItems
    extend self

    def flag_dupes      
      flagjob = Kiba.parse do
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
      Kiba.run(flagjob)
    end

    def one_cat_report
      flag_dupes unless File.file?("#{DATADIR}/working/acqitem_duplicates_flagged.tsv")
      onecatreport = Kiba.parse do
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
      Kiba.run(onecatreport)
    end

    def duplicates
      flag_dupes unless File.file?("#{DATADIR}/working/acqitem_duplicates_flagged.tsv")
      
      dupeacqcatjob = Kiba.parse do
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
      Kiba.run(dupeacqcatjob)
    end

    def uncat_items
      flag_dupes unless File.file?("#{DATADIR}/working/acqitem_duplicates_flagged.tsv")
      acqitemcatjob = Kiba.parse do
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


        transform Rename::Field, from: :id_number, to: :objectnumber
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
            row[:briefdescription] = summary.split('LINEBREAKWASHERE').join(' -- ')
          else
            row[:briefdescription] = nil
          end
          row
        end

        # SECTION below adds columns used in collectionobject csv
        # It is unnecessary if we can't combine two data sources
        transform Merge::ConstantValue, target: :datasource, value: 'acqitem'
        # END SECTION

        transform Delete::FieldsExcept, keepfields: %i[objectnumber assocstructureddategroup assocdatetype
                                                       inventorystatus briefdescription]

        #show_me!
        
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/acqitem_collectionobjects.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          initial_headers: %i[objectnumber],
          csv_options: TSVOPT
        
        post_process do
          puts "\n\nUNCAT OBJECT RECORDS FROM ACQ ITEMS"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(acqitemcatjob)
    end

    def id_lookup
      acqitemkeys = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @deduper = {}
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/acquisition_items.tsv", csv_options: TSVOPT
        # Ruby's CSV gives us "CSV::Row" but we want Hash
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        # this only processes rows with id_number
        transform FilterRows::FieldPopulated, action: :keep, field: :id_number

        transform Delete::FieldsExcept, keepfields: %i[akey id_number m_id]
        #show_me!
        
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/acqitem_link.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        
        post_process do
          puts "\n\nACQUISITION ITEM LINKAGE"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end  
      end
      Kiba.run(acqitemkeys)
    end

    def no_acquisition
      acq_items_no_acq = Kiba.parse do
        @srcrows = 0
        @outrows = 0

        @acq = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/acquisitions.tsv",
                                        csvopt: TSVOPT,
                                        keycolumn: :akey)
        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/mimsy/acquisition_items.tsv",
          csv_options: TSVOPT

        transform{ |r| r.to_h }

        transform{ |r| @srcrows += 1; r }

        transform Merge::CountOfMatchingRows,
          lookup: @acq,
          keycolumn: :akey,
          targetfield: :a_ct

        transform FilterRows::FieldEqualTo, action: :keep, field: :a_ct, value: 0

        transform{ |r| @outrows += 1; r }
        
        filename = "#{DATADIR}/reports/acq_items_no_acq.tsv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
        
        post_process do
          puts "\n\nACQ ITEMS WITH NO ACQ"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(acq_items_no_acq)
    end

    def one_acquisition
      acq_items_one_acq = Kiba.parse do
        @srcrows = 0
        @outrows = 0

        @acq = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/acquisitions.tsv",
                                        csvopt: TSVOPT,
                                        keycolumn: :akey)
        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/mimsy/acquisition_items.tsv",
          csv_options: TSVOPT

        transform{ |r| r.to_h }

        transform{ |r| @srcrows += 1; r }

        transform Merge::CountOfMatchingRows,
          lookup: @acq,
          keycolumn: :akey,
          targetfield: :a_ct

        transform FilterRows::FieldEqualTo, action: :keep, field: :a_ct, value: 1

        transform{ |r| @outrows += 1; r }
        
        filename = "#{DATADIR}/reports_ok/acq_items_one_acq.tsv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
        
        post_process do
          puts "\n\nACQ ITEMS WITH ONE ACQ"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(acq_items_one_acq)
    end

    def multi_acquisition
      acq_items_multiple_acq = Kiba.parse do
        @srcrows = 0
        @outrows = 0

        @acq = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/acquisitions.tsv",
                                        csvopt: TSVOPT,
                                        keycolumn: :akey)
        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/mimsy/acquisition_items.tsv",
          csv_options: TSVOPT

        transform{ |r| r.to_h }

        transform{ |r| @srcrows += 1; r }

        transform Merge::CountOfMatchingRows,
          lookup: @acq,
          keycolumn: :akey,
          targetfield: :a_ct

        transform FilterRows::FieldValueGreaterThan, action: :keep, field: :a_ct, value: 1

        transform{ |r| @outrows += 1; r }
        
        filename = "#{DATADIR}/reports/acq_items_multiple_acq.tsv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
        
        post_process do
          puts "\n\nACQ ITEMS WITH MULTIPLE ACQ"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(acq_items_multiple_acq)
    end

    def no_cat
      acq_items_no_cat = Kiba.parse do
        @srcrows = 0
        @outrows = 0
        
        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/mimsy/acquisition_items.tsv",
          csv_options: TSVOPT

        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldPopulated, action: :reject, field: :m_id
        transform{ |r| @outrows += 1; r }
        
        filename = "#{DATADIR}/reports_ok/acq_items_no_cat.tsv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
        
        post_process do
          puts "\n\nACQ ITEMS WITH NO CAT"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(acq_items_no_cat)
    end

    def one_cat
      acq_items_one_cat = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe

        @srcrows = 0
        @outrows = 0

        @cat = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/catalogue.tsv",
                                        csvopt: TSVOPT,
                                        keycolumn: :mkey)

        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/mimsy/acquisition_items.tsv",
          csv_options: TSVOPT

        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldPopulated, action: :keep, field: :m_id
        transform Rename::Field, from: :m_id, to: :mkey

        transform Merge::CountOfMatchingRows,
          lookup: @cat,
          keycolumn: :mkey,
          targetfield: :cat_ct

        transform FilterRows::FieldEqualTo,
          action: :keep,
          field: :cat_ct,
          value: 1

        #  show_me!

        transform{ |r| @outrows += 1; r }
        
        filename = "#{DATADIR}/reports_ok/acq_items_one_cat.tsv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
        
        post_process do
          puts "\n\nACQ ITEMS WITH ONE CAT"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(acq_items_one_cat)
    end
    
    def multi_cat
      acq_items_multiple_cat = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe

        @srcrows = 0
        @outrows = 0

        @cat = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/catalogue.tsv",
                                        csvopt: TSVOPT,
                                        keycolumn: :mkey)

        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/mimsy/acquisition_items.tsv",
          csv_options: TSVOPT

        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldPopulated, action: :keep, field: :m_id
        transform Rename::Field, from: :m_id, to: :mkey

        transform Merge::CountOfMatchingRows,
          lookup: @cat,
          keycolumn: :mkey,
          targetfield: :cat_ct

        transform FilterRows::FieldValueGreaterThan,
          action: :keep,
          field: :cat_ct,
          value: 1

        show_me!

        transform{ |r| @outrows += 1; r }
        
        filename = "#{DATADIR}/reports/acq_items_multiple_cat.tsv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
        
        post_process do
          puts "\n\nACQ ITEMS WITH MULTIPLE CAT"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(acq_items_multiple_cat)
    end
  end
end
