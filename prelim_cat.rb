require_relative 'config'

# @remove_loans -- removes loan rows as per
#   #https://3.basecamp.com/3410311/buckets/16953827/todos/2708290043
# @test_recs -- keep only records with mkeys listed in provided/co_select files
module Mimsy
  module Cat
    def self.setup
      # Use either make_working or limit_to_test_records
      #Mimsy::Cat.make_working
      Mimsy::Cat.limit_to_test_records
      Mimsy::Cat.exclude_loans
    end

    def self.prep_item_names
      @job = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/mimsy/item_names.tsv",
          csv_options: TSVOPT
        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldEqualTo, action: :reject, field: :prior_name, value: 'Y'
        transform FilterRows::FieldPopulated, action: :keep, field: :item_name
        transform Delete::Fields, fields: %i[line_number id prior_name]
        
        #show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/item_names.tsv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
        post_process do
          label = 'prelim_cat/prep_item_names'
          puts "\n\n#{label.upcase}"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(@job)
    end

    def self.make_working
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

    def self.limit_to_test_records
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
        # @reltest = Lookup.csv_to_multi_hash(file: "#{DATADIR}/provided/co_select_rels.tsv",
        #                                     csvopt: TSVOPT,
        #                                     keycolumn: :mkey)

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
        # transform Merge::MultiRowLookup,
        #   lookup: @reltest,
        #   keycolumn: :mkey,
        #   fieldmap: {
        #     :keeprel => :mkey
        #   }

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

    def self.exclude_loans
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
