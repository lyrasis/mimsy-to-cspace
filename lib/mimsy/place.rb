# frozen_string_literal: true

# normalized_place_lookup
#  - writes place_norm_lookup.tsv (place, normplace, duplicate)
#  - this matches strings in object record to deduplicated authority terms 

module Mimsy
  module Place
    extend self

    def normalized_place_lookup
      Mimsy::Cat.setup unless File.file?("#{DATADIR}/working/catalogue.tsv")
      extract_norm_places = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0
        @deduper = {}
        @normdeduper = {}
        
        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/catalogue.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        # SECTION BELOW gets place values from catalogue.tsv columns, combines them into one multival column,
        #  and then makes each value from that column its own row
        transform Delete::FieldsExcept, keepfields: %i[place_collected place_made]
        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[place_collected place_made],
          target: :place,
          sep: '|'
        transform FilterRows::FieldPopulated, action: :keep, field: :place
        transform Explode::RowsFromMultivalField, field: :place, delim: '|'
        # END SECTION

        # SECTION BELOW deduplicates the list of places
        transform Deduplicate::Flag, on_field: :place, in_field: :duplicate, using: @deduper
        transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate, value: 'n'
        transform Delete::Fields, fields: %i[duplicate]
        # END SECTION

        transform Cspace::FlagInvalidCharacters, check: :place, flag: :flag
        
        # SECTION BELOW adds column with place normalized for CSpace ID
        transform Cspace::NormalizeForID, source: :place, target: :normplace
        transform Deduplicate::Flag, on_field: :normplace, in_field: :duplicate, using: @normdeduper
        # END SECTION
        
        #show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/place_norm_lookup.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        post_process do
          puts "\n\nPLACES FROM OBJECTS NORMALIZED LOOKUP"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(extract_norm_places)
    end
  end
end
