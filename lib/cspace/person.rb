# frozen_string_literal: true

# csv
#  - export CSV of person authorities for CollectionSpace import

module Cspace
  module Person
    extend self

    def csv
      Mimsy::People.all_people unless File.file?("#{DATADIR}/working/people.tsv")

      personjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/people.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }
        
        transform FilterRows::FieldEqualTo, action: :keep, field: :individual, value: 'Y'
        transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate, value: 'n'
        
        # Uncomment if variations are being treated as alternate term forms
        # transform Copy::Field, from: :preferred_name, to: :termName
        transform Rename::Field, from: :birth_date, to: :birthDateGroup
        transform Rename::Field, from: :birth_place, to: :birthPlaceLocal
        transform Rename::Field, from: :death_date, to: :deathDateGroup
        transform Rename::Field, from: :death_place, to: :deathPlaceLocal
        transform Rename::Field, from: :firstmid_name, to: :foreName
        transform Rename::Field, from: :lastsuff_name, to: :surName
        transform CombineValues::FromFieldsWithDelimiter, sources: [:suffix_name, :honorary_suffix], target: :nameAdditions, sep: ', '
        transform Rename::Field, from: :note, to: :nameNote
        transform Rename::Field, from: :title_name, to: :title
        transform CombineValues::FromFieldsWithDelimiter, sources: [:brief_bio, :description], target: :bioNote, sep: ' --- '

        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[nameNote bioNote],
          find: 'LINEBREAKWASHERE',
          replace: "\n"
        

        transform Delete::Fields, fields: [:individual, :link_id, :duplicate, :preferred_name]

        # transform FilterRows::FieldPopulated, action: :keep, field: :birthDateGroup
        # show_me!
        transform{ |r| @outrows += 1; r }
        
        filename = 'data/cs/person.csv'
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: CSVOPT
        
        post_process do
          puts "\n\nPERSON AUTHORITIES"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end

      Kiba.run(personjob)
    end
  end
end
