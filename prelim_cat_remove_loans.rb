require_relative 'config'

module Mimsy
  module Cat
    @remove_loans = Kiba.parse do
      extend Kiba::Common::DSLExtensions::ShowMe
      
      @srcrows = 0
      @outrows = 0

      source Kiba::Common::Sources::CSV,
        filename: "#{DATADIR}/mimsy/catalogue.tsv",
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

      #  show_me!
      
      transform{ |r| @outrows += 1; r }
      
      filename = "#{DATADIR}/working/catalogue.tsv"
      destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
      
      post_process do
        puts "\n\nCAT WITH Loan items removed"
        puts "#{@outrows} (of #{@srcrows})"
        puts "file: #{filename}"
      end
    end

    def self.setup
      # removes loan rows as per
      # https://3.basecamp.com/3410311/buckets/16953827/todos/2708290043
      Kiba.run(@remove_loans)
    end
  end
end
