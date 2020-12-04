# frozen_string_literal: true

# Purpose: extract and prepare "Credit Line" info from Mimsy catalogue table for import as
#          CollectionSpace work authorities for use in namedCollection field
# flag
#  - produce list of unique original (i.e. NOT normalized) work names
#  - add normalized form
#  - flag duplicates on normalized form
# duplicates
#  - writes report of work names that will not be imported due to duplicate normalized forms
# unique
#  - writes out list of non-duplicate work names that will be imported as authorities
# lookup
#  Purpose: used to match string value from catalogue data to the appropriate preferred
#           authority string
#  - takes result of flag, merges in non-normalized form from unique using match on normalized form
# csv
#  - converts unique to csv ready for CollectionSpace import
module Cspace
  module Work
    extend self

    def flag
      Mimsy::Cat.setup unless File.file?("#{DATADIR}/working/catalogue.tsv")
      flagjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @deduper = {}
        @norm_deduper = {}
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/catalogue.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform Delete::FieldsExcept, keepfields: %i[credit_line]
        transform Rename::Field, from: :credit_line, to: :collection
        transform FilterRows::FieldPopulated, action: :keep, field: :collection
        transform Deduplicate::Flag, on_field: :collection, in_field: :duplicate, using: @deduper
        transform FilterRows::FieldEqualTo, action: :reject, field: :duplicate, value: 'y'
        transform Delete::Fields, fields: %i[duplicate]
        
        transform Cspace::NormalizeForID, source: :collection, target: :norm_coll
        transform Deduplicate::Flag, on_field: :norm_coll, in_field: :duplicate, using: @norm_deduper

        #show_me!
        
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/works_flagged.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        
        post_process do
          label = 'Unique work names with duplicate norm forms flagged'
          puts "\n\n#{label.upcase}"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(flagjob)
    end

    def duplicates
      flag unless File.file?("#{DATADIR}/working/works_flagged.tsv")
      dupejob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/works_flagged.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate, value: 'y'
        
        #show_me!
        
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/reports/works_DUPLICATE.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        
        post_process do
          label = 'Duplicate works'
          puts "\n\n#{label.upcase}"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(dupejob)
    end
    
    def unique
      flag unless File.file?("#{DATADIR}/working/works_flagged.tsv")
      uniqjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/works_flagged.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldEqualTo, action: :reject, field: :duplicate, value: 'y'
        transform Delete::Fields, fields: %i[duplicate]
        
        #show_me!
        
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/works_uniq.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        
        post_process do
          label = 'Unique works for mapping to CS'
          puts "\n\n#{label.upcase}"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(uniqjob)
    end
    
    def lookup
      unique unless File.file?("#{DATADIR}/working/works_uniq.tsv")
      lkjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        @wrk = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/works_uniq.tsv",
                                        csvopt: TSVOPT,
                                        keycolumn: :norm_coll)


        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/works_flagged.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform Merge::MultiRowLookup,
          lookup: @wrk,
          keycolumn: :norm_coll,
          fieldmap: {
            :use_value => :collection
          }

        #show_me!
        
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/works_lookup.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        
        post_process do
          label = 'Lookup table for merging into object data'
          puts "\n\n#{label.upcase}"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(lkjob)
    end

    def csv
      unique unless File.file?("#{DATADIR}/working/works_uniq.tsv")
      csvjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        @alt = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/works_flagged.tsv",
                                        csvopt: TSVOPT,
                                        keycolumn: :norm_coll)

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/works_uniq.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform Merge::MultiRowLookup,
          lookup: @alt,
          keycolumn: :norm_coll,
          fieldmap: {
            :alt_names => :collection
          },
          constantmap: {
            termprefforlangnonpreferred: 'n'
          },
          delim: '|'

        transform do |row|
          alt = row.fetch(:alt_names).split('|')
          if alt.length == 1
            alt = nil
            row[:termprefforlangnonpreferred] = nil
          else
            pref = row.fetch(:collection)
            same_as_pref = alt.select{ |e| e == pref }
            unless same_as_pref.empty?
              alt = alt - same_as_pref
              np = row[:termprefforlangnonpreferred].split('|')
              same_as_pref.length.times{ np.shift }
              row[:termprefforlangnonpreferred] = np.join(';')
            end
            alt = alt.map{ |e| e.gsub(';', ',') }.join(';')
          end
          row[:alt_names] = alt
          row
        end

        transform Delete::Fields, fields: %i[norm_coll]
        transform Rename::Field, from: :collection, to: :termdisplayname
        transform Rename::Field, from: :alt_names, to: :termdisplaynamenonpreferred
        transform Merge::ConstantValue, target: :termprefforlang, value: 'y'
        transform Merge::ConstantValue, target: :worktype, value: 'Collection'
        #show_me!
        
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/cs/works.csv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: LOCCSVOPT,
          initial_headers: %i[termdisplayname termdisplaynamenonpreferred]
        
        post_process do
          label = 'CollectionSpace Works'
          puts "\n\n#{label.upcase}"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(csvjob)
    end
  end
end
