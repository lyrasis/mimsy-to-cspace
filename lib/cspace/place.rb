# frozen_string_literal: true

# csv
#  - outputs CSV of place authority terms used in Objects
module Cspace
  module Place
    extend self
    def csv
      Mimsy::Place.normalized_place_lookup unless File.file?("#{DATADIR}/working/place_norm_lookup.tsv")
      
      cs_places = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0
        
        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/place_norm_lookup.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate, value: 'n'
        transform Rename::Field, from: :place, to: :termDisplayName
        transform Merge::ConstantValue, target: :termStatus, value: 'provisional'
        transform Merge::ConstantValue, target: :termSourceLocal, value: 'Mimsy export'
        transform Merge::ConstantValue, target: :termSourceDetail, value: 'used in catalogue.csv PLACE_MADE or PLACE_COLLECTED'
        transform Delete::Fields, fields: %i[normplace duplicate flag]
        
        #show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/cs/places.csv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: LOCCSVOPT
        post_process do
          puts "\n\nPLACES FROM OBJECTS"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(cs_places)
    end
  end
end
