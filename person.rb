require_relative 'config'

# conversion of Mimsy people table to CollectionSpace Person authority csv
contactsjob = Kiba.parse do
  source Kiba::Common::Sources::CSV, filename: 'data/mimsy/people_contacts.tsv', csv_options: TSVOPT
  transform { |r| r.to_h }
#  transform SelectRows::WithFieldEqualTo, action: :keep, field: :id, value: '78'
  transform Clean::FieldClean,
    fields: [:address1, :address2, :address3],
    instructions: {
      :replace => {
        '^x+$' => ''
      },
      :remove_if_contains => [
        '[Uu]nknown',
        '[Dd]eceased'
        ]
    }
  transform ConcatColumns, sources: [:department, :address1], target: :combinedaddress1, sep: ' --- '
  transform ConcatColumns, sources: [:address2, :address3], target: :combinedaddress2, sep: ' --- '
  transform Reshape::CombineAndTypeFields,
    sourcefieldmap: {
      :work_phone => 'business',
      :home_phone => 'home'
    },
    datafield: :phone,
    typefield: :phone_type
  
    extend Kiba::Common::DSLExtensions::ShowMe
    show_me!

    filename = 'data/working/people_contacts.tsv'
    destination Kiba::Common::Destinations::CSV, filename: filename, csv_options: TSVOPT
    post_process do
      puts "File generated in #{filename}"
    end
end


personjob = Kiba.parse do
  @varnames = Lookup.csv_to_multi_hash(file: 'data/mimsy/people_variations.tsv', keycolumn: :link_id)
  @contacts = Lookup.csv_to_multi_hash(file: 'data/working/people_contacts.tsv', keycolumn: :link_id)
  source Kiba::Common::Sources::CSV, filename: 'data/mimsy/people.tsv', csv_options: TSVOPT
  # Ruby's CSV gives us "CSV::Row" but we want Hash
  transform { |r| r.to_h }

  transform SelectRows::WithFieldEqualTo, action: :keep, field: :individual, value: 'Y'
  transform SelectRows::WithFieldEqualTo, action: :reject, field: :link_id, value: '-9999'
  transform OneColumnToMulti, source: :preferred_name, targets: [:termDisplayName, :termName]
  transform RenameField, from: :birth_date, to: :birthDateGroup
  transform RenameField, from: :birth_place, to: :birthPlace
  transform RenameField, from: :death_date, to: :deathDateGroup
  transform RenameField, from: :death_place, to: :deathPlace
  transform RenameField, from: :firstmid_name, to: :foreName
  transform RenameField, from: :suffix_name, to: :nameAdditions
  transform RenameField, from: :lastsuff_name, to: :surName
  transform RenameField, from: :nationality, to: :nationality
  transform RenameField, from: :note, to: :nameNote
  transform RenameField, from: :title_name, to: :title
  transform ConcatColumns, sources: [:brief_bio, :description], target: :bioNote, sep: ' --- '
  transform AppendStringToFieldValue, target_column: :nameAdditions, source_column: :honorary_suffix, sep: ', '
  transform StaticFieldValueMapping, source: :language, target: :termLanguage, mapping: LANGUAGES
  transform StaticFieldValueMapping, source: :gender, target: :gender, mapping: GENDER, delete_source: false
  transform ConstantValue, target: :termPrefForLang, value: 'true'
  transform ConstantValue, target: :termSourceLocal, value: 'Mimsy'
  transform ConstantValue, target: :termSourceDetail, value: 'people.csv/PREFERRED_NAME'
  transform Lookup::MultiRowLookupMerge,
    fieldmap: {:termDisplayNameNonPreferred => :variation},
    constantmap: {
      :termPrefForLangNonPreferred => 'false',
      :termSourceLocalNonPreferred => 'Mimsy',
      :termSourceDetailNonPreferred => 'people_variation.csv/VARIATION'
    },
    lookup: @varnames,
    keycolumn: :link_id,
    exclusion_criteria: {
      :equal => {
        :termDisplayName => :variation
      }
    }
  transform Lookup::MultiRowLookupMerge,
    fieldmap: {:addressPlace1 => :combinedaddress1,
               :addressPlace2 => :combinedaddress2,
               :addressMunicipality => :city,
               :addressStateOrProvince => :state_province,
               :addressPostCode => :postal_code,
               :addressCountry => :country,
               :faxNumber => :fax,
               :email => :e_mail,
               :webAddress => :www_address,
               :telephoneNumber => :phone,
               :telephoneNumberType => :phone_type
               },
    lookup: @contacts,
    keycolumn: :link_id
  transform DeleteFields, [:approved, :individual, :deceased, :link_id]

  # useful to show the records as they flow:
  
  extend Kiba::Common::DSLExtensions::ShowMe
  show_me!

  filename = 'data/cs/person.csv'
  destination Kiba::Common::Destinations::CSV, filename: filename, csv_options: CSVOUTOPT
  
  post_process do
    puts "File generated in #{filename}"
  end
end

Kiba.run(contactsjob)
Kiba.run(personjob)
