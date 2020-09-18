# frozen_string_literal: true

# csv
#  - outputs location authorities ready for CollectionSpace import
# hierarchies
#  - outputs location hierarchy relationships ready for CollectionSpace import
module Cspace
  module Location
    extend self

    def csv
    locs = Kiba.parse do
      extend Kiba::Common::DSLExtensions::ShowMe
      @srcrows = 0
      @outrows = 0
      @deduper = {}

      source Kiba::Common::Sources::CSV,
        filename: "#{DATADIR}/provided/norm_location_mapping.tsv",
        csv_options: TSVOPT
      transform{ |r| r.to_h }
      transform{ |r| @srcrows += 1; r }

      transform Deduplicate::Flag, on_field: :loc_auth, in_field: :duplicate, using: @deduper
      transform FilterRows::FieldEqualTo, action: :reject, field: :duplicate, value: 'y'
      transform Delete::FieldsExcept, keepfields: %i[loc_auth]
      transform FilterRows::FieldPopulated, action: :keep, field: :loc_auth
      transform Rename::Field, from: :loc_auth, to: :termdisplayname
      transform Merge::ConstantValue, target: :termlanguage, value: 'English'
      transform Merge::ConstantValue, target: :termprefforlang, value: 'true'
      transform Merge::ConstantValue, target: :termstatus, value: 'accepted'
      transform Merge::ConstantValue, target: :termflag, value: 'full term'
      
      transform{ |r| @outrows += 1; r }

      filename = "#{DATADIR}/cs/locations.csv"
      destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: CSVOPT
      
      post_process do
        puts "\n\nCSPACE LOCATIONS"
        puts "#{@outrows} (of #{@srcrows})"
        puts "file: #{filename}"
      end
    end
    Kiba.run(locs)
    end

    def hierarchies
      hiers = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0
        @deduper = {}

        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/provided/loc_hier_lookup.tsv",
          csv_options: TSVOPT
        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldEqualTo, action: :reject, field: :directparent, value: 'n/a'
        transform Delete::FieldsExcept, keepfields: %i[narrowestloc directparent]
        transform Rename::Field, from: :narrowestloc, to: :narrower
        transform Rename::Field, from: :directparent, to: :broader
        transform Merge::ConstantValue, target: :type, value: 'Location'
        transform Merge::ConstantValue, target: :subtype, value: 'location'
        
        transform{ |r| @outrows += 1; r }

        filename = "#{DATADIR}/cs/rels_hier_locations.csv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: CSVOPT
        
        post_process do
          puts "\n\nCSPACE LOCATION HIERARCHY"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(hiers)
    end
  end
end
