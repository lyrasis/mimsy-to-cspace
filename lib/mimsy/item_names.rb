# frozen_string_literal: true

# prep
#  - removes prior values, rows with blank item name
#  - prepares additional item names for merge into main object record

module Mimsy
  module ItemNames
    extend self

    def prep
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
  end
end
