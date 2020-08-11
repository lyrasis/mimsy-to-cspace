require_relative 'config'
require_relative 'prelim_people_build'

Mimsy::People.setup

personjob = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe
  
  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/people.tsv", csv_options: TSVOPT
  # Ruby's CSV gives us "CSV::Row" but we want Hash
  transform { |r| r.to_h }

  transform FilterRows::FieldEqualTo, action: :keep, field: :individual, value: 'Y'
  transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate, value: 'n'

    
  transform CombineValues::FullRecord, target: :search
  transform FilterRows::FieldMatchRegexp, action: :keep, field: :search, match: 'LINEBREAKWASHERE'
  transform Delete::Fields, fields: %i[search]
  
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

  filename = 'data/cs/person.csv'
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
    csv_options: CSVOPT
    
  post_process do
    puts "File generated in #{filename}"
  end
end

Kiba.run(personjob)
