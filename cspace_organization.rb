require_relative 'config'
require_relative 'prelim_people_build'

Mimsy::People.setup

orgjob = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe

  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/people.tsv", csv_options: TSVOPT
  # Ruby's CSV gives us "CSV::Row" but we want Hash
  transform { |r| r.to_h }

  transform FilterRows::FieldEqualTo, action: :keep, field: :individual, value: 'N'
  transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate, value: 'n'
  
  transform Delete::FieldValueIfEqualsOtherField,
    delete: :lastsuff_name,
    if_equal_to: :termdisplayname

  transform Delete::FieldValueIfEqualsOtherField,
    delete: :contactname,
    if_equal_to: :termdisplayname

  # The following are only used in records erroneously coded as INDIVIDUAL = N
  #  I want to shove them in the note field, with their field names prepended to
  #  to each value to avoid data loss and assist with manual cleanup, if relevant
  transform CombineValues::FromFieldsWithDelimiter,
    sources: [:title_name, :firstmid_name, :lastsuff_name, :birth_date, :birth_place, :death_date,
              :death_place, :gender, :nationality, :occupation, :brief_bio, :description],
    target: :historyNote,
    sep: ' --- ',
    prepend_source_field_name: true

  # The following are not populated in rows coded INDIVIDUAL = N
  transform Delete::Fields, fields: [:suffix_name, :honorary_suffix, :note]

  # The following are populated in rows coded INDIVIDUAL = N but with no other data that could
  #   be mapped to CSpace data model.
  transform Delete::Fields, fields: [:deceased]

  # The following aren't mapped
  transform Delete::Fields, fields: [:individual, :link_id, :duplicate, :preferred_name]

#    show_me!

    filename = 'data/cs/organizations.csv'
    destination Kiba::Extend::Destinations::CSV,
      filename: filename,
      initial_headers: [:termdisplayname],
      csv_options: LOCCSVOPT
    
    post_process do
      puts "File generated in #{filename}"
    end
end

Kiba.run(orgjob)
