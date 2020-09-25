# frozen_string_literal: true

# working
#  - creates a copy of acquisitions.tsv that we can use to merge in reshaped data
# add_sources
#  - creates working copy of acquisition_sources with preferred_name column merged in from people
# id_lookup
#  Purpose: use to create object-acquisition relationships
#  Keeps only rows with acquisition reference number, deduplicates on reference number
#  outputs only acquisition record ID and acquisition reference number
# no_acquisition_items
#  - Writes report of acquisitions not linked to any acquisition items.

module Mimsy
  module Acquisition
    extend self
    
    def working
      wrkacq = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/acquisitions.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform{ |r| @outrows += 1; r }
        filename = 'data/working/acquisitions.tsv'
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        post_process do
          puts "\n\nACQUISITIONS WORKING COPY"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(wrkacq)
    end

    def add_sources
      namesjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0
        @names = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/people.tsv",
                                          csvopt: TSVOPT,
                                          keycolumn: :link_id)

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/acquisition_sources.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform Merge::MultiRowLookup,
          lookup: @names,
          keycolumn: :link_id,
          fieldmap: {
            :preferred_name => :preferred_name,
            :individual => :individual
          }
        
        #show_me!
        transform{ |r| @outrows += 1; r }
        filename = 'data/working/acquisition_sources.tsv'
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        post_process do
          puts "\n\nACQUISITIONS WITH SOURCES"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(namesjob)
    end

    def id_lookup
      acqkeys = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @deduper = {}
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/acquisitions.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        # this only processes rows with ref_number
        transform FilterRows::FieldPopulated, action: :keep, field: :ref_number

        # flag duplicates and remove
        transform Deduplicate::Flag, on_field: :ref_number, in_field: :duplicate, using: @deduper
        transform FilterRows::FieldEqualTo, action: :reject, field: :duplicate, value: 'y'

        transform Delete::FieldsExcept, keepfields: %i[akey ref_number]
        #show_me!
        
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/acq_link.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        
        post_process do
          puts "\n\nACQUISITION LINKAGE"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end  
      end
      Kiba.run(acqkeys)
    end

    def no_acquisition_items
      acq_no_acq_items = Kiba.parse do
        @srcrows = 0
        @outrows = 0

        @acqitems = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/acquisition_items.tsv",
                                             csvopt: TSVOPT,
                                             keycolumn: :akey)
        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/mimsy/acquisitions.tsv",
          csv_options: TSVOPT

        transform{ |r| r.to_h }

        transform{ |r| @srcrows += 1; r }

        transform Merge::CountOfMatchingRows,
          lookup: @acqitems,
          keycolumn: :akey,
          targetfield: :ai_ct

        transform FilterRows::FieldEqualTo, action: :keep, field: :ai_ct, value: 0

        transform{ |r| @outrows += 1; r }
        
        filename = "#{DATADIR}/reports/acq_no_acq_items.tsv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
        
        post_process do
          puts "\n\nACQ WITH NO ACQ ITEMS"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(acq_no_acq_items)
    end
  end
end
