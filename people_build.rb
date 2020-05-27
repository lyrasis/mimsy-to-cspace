require_relative 'config'

# conversion of people_contacts in a more CSpace friendly form for merging to mondo people table
contacts_job = Kiba.parse do
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
    targetsep: MVDELIM
  
    # extend Kiba::Common::DSLExtensions::ShowMe
    # show_me!

    filename = 'data/working/people_contacts.csv'
    destination Kiba::Extend::Destinations::CSV,
      filename: filename,
      csv_options: TSVOPT
    post_process do
      puts "File generated in #{filename}"
    end
end

Kiba.run(contacts_job)

# Builds mondo people table from:
# people, people_variations, people_contacts, acquisition_sources, disposal_sources,
# items_people_sources, items_makers, source_details
people_job = Kiba.parse do
  @varnames = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/people_variations.tsv",
                                       csvopt: TSVOPT,
                                       keycolumn: :link_id)
  @contacts = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/people_contacts.csv",
                                        csvopt: TSVOPT,
                                       keycolumn: :link_id)
  @acqsrc = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/acquisition_sources.tsv",
                                        csvopt: TSVOPT,
                                       keycolumn: :link_id)

  source Kiba::Common::Sources::CSV,
    filename: "#{DATADIR}/mimsy/people.tsv",
    csv_options: TSVOPT
  transform { |r| r.to_h }

  # This is a weird, seemingly invalid record
  transform FilterRows::FieldEqualTo, action: :reject, field: :link_id, value: '-9999'

#  transform FilterRows::FieldEqualTo, action: :keep, field: :link_id, value: '134'
  
  transform Rename::Field, from: :preferred_name, to: :termDisplayName
  transform Delete::Fields, fields: [:approved, :deceased]

  transform Replace::FieldValueWithStaticMapping, source: :language, target: :termLanguage, mapping: LANGUAGES
  transform Replace::FieldValueWithStaticMapping, source: :gender, target: :gender, mapping: GENDER, delete_source: false
  transform Merge::ConstantValue, target: :termPrefForLang, value: 'true'
  transform Merge::ConstantValue, target: :termSourceLocal, value: 'Mimsy export'
  transform Merge::ConstantValue, target: :termSourceDetail, value: 'people.csv/PREFERRED_NAME'

  # merge name variants from people_variants
  transform Merge::MultiRowLookup,
    lookup: @varnames,
    keycolumn: :link_id,
    fieldmap: {:termDisplayNameNonPreferred => :variation},
    constantmap: {
      :termPrefForLangNonPreferred => 'false',
      :termSourceLocalNonPreferred => 'Mimsy export',
      :termSourceDetailNonPreferred => 'people_variation.csv/VARIATION'
    },
    exclusion_criteria: {
      :field_equal => {
        :termDisplayName => :variation
      }
    },
    delim: MVDELIM

  # merge contact address info from people_contacts
  # done together because this needs to be a repeatable group in CSpace
  transform Merge::MultiRowLookup,
    lookup: @contacts,
    keycolumn: :link_id,
    fieldmap: {
      :addressPlace1 => :combinedaddress1,
      :addressPlace2 => :combinedaddress2,
      :addressMunicipality => :city,
      :addressStateOrProvince => :state_province,
      :addressPostCode => :postal_code,
      :addressCountry => :country
    },
    delim: MVDELIM

  # merge the other info from people_contacts
  transform Merge::MultiRowLookup,
    lookup: @contacts,
    keycolumn: :link_id,
    fieldmap: {
               :faxNumber => :fax,
               :email => :e_mail,
               :webAddress => :www_address,
               :telephoneNumber => :phone,
               :telephoneNumberType => :phone_type,
               :contactName => :contact
             },
    delim: MVDELIM

  # merge variant names in from acquisition_sources
  transform Merge::MultiRowLookup,
    lookup: @acqsrc,
    keycolumn: :link_id,
    fieldmap: {
      :ac_variant => :source
    },
    exclusion_criteria: {
      :field_equal => {
        :termDisplayName => :source
      }
    },
    constantmap: {
      :ac_termPrefForLangNonPreferred => 'false',
      :ac_termSourceLocalNonPreferred => 'Mimsy export',
      :ac_termSourceDetailNonPreferred => 'acquisition_sources.tsv/SOURCE'
    },
    delim: MVDELIM

  # merge role/occupation in from acquisition_sources
  transform Merge::MultiRowLookup,
    lookup: @acqsrc,
    keycolumn: :link_id,
    fieldmap: {
      :ac_occupation => :source_role
    },
    delim: MVDELIM

  # merge contact name in from acquisition_sources
  transform Merge::MultiRowLookup,
    lookup: @acqsrc,
    keycolumn: :link_id,
    fieldmap: {
      :ac_contactName => :contact
    },
    delim: MVDELIM

  # omitting merging address since it is very sparsely populated and not formatted in such a way
  #  that we could easily compare to values from people_contacts

  transform Clean::DowncaseFieldValues,
    fields: [:ac_occupation]

  transform Deduplicate::FieldValues,
    fields: [:ac_occupation],
    sep: MVDELIM

  transform FilterRows::FieldPopulated, action: :keep, field: :ac_variant

#  transform Clean::DelimiterOnlyFields, delim: MVDELIM
  
  extend Kiba::Common::DSLExtensions::ShowMe
  show_me!

  filename = 'data/working/people.tsv'
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
    csv_options: TSVOPT
  post_process do
    puts "File generated in #{filename}"
  end
end

Kiba.run(people_job)
