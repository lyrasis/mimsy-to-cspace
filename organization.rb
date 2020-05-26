require_relative 'config'

# conversion of Mimsy people table to CollectionSpace Organization authority csv
# ASSUMES PERSON ETL HAS ALREADY BEEN RUN -- If not, run it to create the
#   contacts working table
orgjob = Kiba.parse do
  @varnames = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/people_variations.tsv",
                                       csvopt: TSVOPT,
                                       keycolumn: :link_id)
  @contacts = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/people_contacts.tsv",
                                       csvopt: {headers: true, header_converters: :symbol},
                                       keycolumn: :link_id)
  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/people.tsv", csv_options: TSVOPT
  # Ruby's CSV gives us "CSV::Row" but we want Hash
  transform { |r| r.to_h }

  transform FilterRows::FieldEqualTo, action: :keep, field: :individual, value: 'N'
  # Uncomment if variations are being treated as alternate term forms
  transform Copy::Field, from: :preferred_name, to: :termName

  transform Copy::Field, from: :preferred_name, to: :mainBodyName
  transform Rename::Field, from: :preferred_name, to: :termDisplayName
  
  transform Replace::FieldValueWithStaticMapping, source: :language, target: :termLanguage, mapping: LANGUAGES
  transform Merge::ConstantValue, target: :termPrefForLang, value: 'true'
  transform Merge::ConstantValue, target: :termSourceLocal, value: 'Mimsy export'
  transform Merge::ConstantValue, target: :termSourceDetail, value: 'people.csv/PREFERRED_NAME where INDIVIDUAL = N'
  # This puts the first value from variations that does not match preferred term in :termName
  # transform Merge::MultiRowLookup,
  #   fieldmap: {:termName => :variation},
  #   lookup: @varnames,
  #   keycolumn: :link_id,
  #   exclusion_criteria: {
  #     :field_equal => {
  #       :termDisplayName => :variation
  #     }
  #   },
  #   selection_criteria: {
  #     :position => 'first'
  #   }
  # Keep in case they want to treat variations as alternate term forms
  transform Merge::MultiRowLookup,
    fieldmap: {:termDisplayNameNonPreferred => :variation},
    constantmap: {
      :termPrefForLangNonPreferred => 'false',
      :termSourceLocalNonPreferred => 'Mimsy export',
      :termSourceDetailNonPreferred => 'people_variation.csv/VARIATION'
    },
    lookup: @varnames,
    keycolumn: :link_id,
    exclusion_criteria: {
      :field_equal => {
        :termDisplayName => :variation
      }
    }

  transform Merge::MultiRowLookup,
    fieldmap: {
      :contactName => :contact
    },
    lookup: @contacts,
    keycolumn: :link_id
  
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

  transform Delete::FieldValueMatchingRegexp,
    fields: [:gender],
    match: '^N$'

  transform Delete::FieldValueIfEqualsOtherField,
    delete: :lastsuff_name,
    if_equal_to: :termDisplayName
  
  # The following are only used in records erroneously coded as INDIVIDUAL = N
  #  I want to shove them in the note field, with their field names prepended to
  #  to each value to avoid data loss and assist with manual cleanup, if relevant
  transform CombineValues::FromFieldsWithDelimiter,
    sources: [:title_name, :firstmid_name, :lastsuff_name, :birth_date, :birth_place, :death_date,
              :death_place, :gender, :nationality, :brief_bio, :description],
    target: :historyNote,
    sep: ' --- ',
    prepend_source_field_name: true

  # The following are not populated in rows coded INDIVIDUAL = N
  transform Delete::Fields, fields: [:suffix_name, :honorary_suffix, :note]

  # The following are populated in rows coded INDIVIDUAL = N but with no other data that could
  #   be mapped to CSpace data model.
  transform Delete::Fields, fields: [:deceased]

  # The following aren't mapped
  transform Delete::Fields, fields: [:approved, :individual, :link_id]

#  extend Kiba::Common::DSLExtensions::ShowMe
#  show_me!

  filename = 'data/cs/organizations.csv'
  destination Kiba::Extend::Destinations::CSV, filename: filename, initial_headers: [:termDisplayName]
  
  post_process do
    puts "File generated in #{filename}"
  end
end

Kiba.run(orgjob)
