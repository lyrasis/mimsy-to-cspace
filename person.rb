require_relative 'config'

# conversion of Mimsy people table to CollectionSpace Person authority csv
contactsjob = Kiba.parse do
  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/people_contacts.tsv", csv_options: TSVOPT
  transform { |r| r.to_h }
#  transform FilterRows::FieldEqualTo, action: :keep, field: :id, value: '78'
  transform Clean::RegexpFindReplaceFieldVals,
    fields: [:address1, :address2, :address3],
    find: '^x+$',
    replace: ''
  transform Delete::FieldValueContainingString,
    fields: [:address1, :address2, :address3],
    match: 'unknown',
    casesensitive: false
  transform Delete::FieldValueContainingString,
    fields: [:address1, :address2, :address3],
    match: 'deceased',
    casesensitive: false
  

  transform CombineValues::FromFieldsWithDelimiter, sources: [:department, :address1], target: :combinedaddress1, sep: ' --- '
  transform CombineValues::FromFieldsWithDelimiter, sources: [:address2, :address3], target: :combinedaddress2, sep: ' --- '
  transform Reshape::CollapseMultipleFieldsToOneTypedFieldPair,
    sourcefieldmap: {
      :work_phone => 'business',
      :home_phone => 'home'
    },
    datafield: :phone,
    typefield: :phone_type,
    targetsep: ';'
  
    extend Kiba::Common::DSLExtensions::ShowMe
    show_me!

    filename = 'data/working/people_contacts.tsv'
    destination Kiba::Common::Destinations::CSV, filename: filename
    post_process do
      puts "File generated in #{filename}"
    end
end


personjob = Kiba.parse do
  @varnames = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/people_variations.tsv",
                                       csvopt: TSVOPT,
                                       keycolumn: :link_id)
  @contacts = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/people_contacts.tsv",
                                       csvopt: {headers: true, header_converters: :symbol},
                                       keycolumn: :link_id)
  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/people.tsv", csv_options: TSVOPT
  # Ruby's CSV gives us "CSV::Row" but we want Hash
  transform { |r| r.to_h }

  transform FilterRows::FieldEqualTo, action: :keep, field: :individual, value: 'Y'
  transform FilterRows::FieldEqualTo, action: :reject, field: :link_id, value: '-9999'
  # Uncomment if variations are being treated as alternate term forms
  # transform Copy::Field, from: :preferred_name, to: :termName
  transform Rename::Field, from: :preferred_name, to: :termDisplayName
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
  transform Replace::FieldValueWithStaticMapping, source: :language, target: :termLanguage, mapping: LANGUAGES
  transform Replace::FieldValueWithStaticMapping, source: :gender, target: :gender, mapping: GENDER, delete_source: false
  transform Merge::ConstantValue, target: :termPrefForLang, value: 'true'
  transform Merge::ConstantValue, target: :termSourceLocal, value: 'Mimsy export'
  transform Merge::ConstantValue, target: :termSourceDetail, value: 'people.csv/PREFERRED_NAME'
  # This puts the first value from variations that does not match preferred term in :termName
  transform Merge::MultiRowLookup,
    fieldmap: {:termName => :variation},
    lookup: @varnames,
    keycolumn: :link_id,
    exclusion_criteria: {
      :field_equal => {
        :termDisplayName => :variation
      }
    },
    selection_criteria: {
      :position => 'first'
    }
  # Keep in case they want to treat variations as alternate term forms
  # transform Merge::MultiRowLookup,
  #   fieldmap: {:termDisplayNameNonPreferred => :variation},
  #   constantmap: {
  #     :termPrefForLangNonPreferred => 'false',
  #     :termSourceLocalNonPreferred => 'Mimsy export',
  #     :termSourceDetailNonPreferred => 'people_variation.csv/VARIATION'
  #   },
  #   lookup: @varnames,
  #   keycolumn: :link_id,
  #   exclusion_criteria: {
  #     :field_equal => {
  #       :termDisplayName => :variation
  #     }
  #  }
  
  transform Merge::MultiRowLookup,
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
  transform Delete::Fields, fields: [:approved, :individual, :deceased, :link_id]

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
