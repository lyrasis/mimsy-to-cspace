require_relative 'config'

personjob = Kiba.parse do
  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/people.tsv", csv_options: TSVOPT
  # Ruby's CSV gives us "CSV::Row" but we want Hash
  transform { |r| r.to_h }

  transform FilterRows::FieldEqualTo, action: :keep, field: :individual, value: 'Y'

  # Uncomment if variations are being treated as alternate term forms
  # transform Copy::Field, from: :preferred_name, to: :termName
  transform Rename::Field, from: :birth_date, to: :birthDateGroup
  transform Rename::Field, from: :birth_place, to: :birthPlaceLocal
  transform Rename::Field, from: :death_date, to: :deathDateGroup
  transform Rename::Field, from: :death_place, to: :deathPlaceLocal
  transform Rename::Field, from: :firstmid_name, to: :foreName
  transform Rename::Field, from: :lastsuff_name, to: :surName
  transform CombineValues::FromFieldsWithDelimiter, sources: [:suffix_name, :honorary_suffix], target: :nameAdditions, sep: ', '
  transform Rename::Field, from: :nationality, to: :nationality
  transform Rename::Field, from: :note, to: :nameNote
  transform Rename::Field, from: :title_name, to: :title
  transform CombineValues::FromFieldsWithDelimiter, sources: [:brief_bio, :description], target: :bioNote, sep: ' --- '
  # Keep in case they want to treat variations as alternate term forms
  
  transform Delete::Fields, fields: [:individual, :link_id]

#  extend Kiba::Common::DSLExtensions::ShowMe
#  show_me!

  filename = 'data/cs/person-variations_first_val_as_name.csv'
  destination Kiba::Extend::Destinations::CSV, filename: filename, initial_headers: [:termDisplayName]
  
  post_process do
    puts "File generated in #{filename}"
  end
end

Kiba.run(contactsjob)
Kiba.run(personjob)
