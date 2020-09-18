# frozen_string_literal: true

# reshape_contacts
#  - conversion of people_contacts in a more CSpace friendly form for merging to mondo
#    people table
# all_people
#  - Builds mondo people table from:
#      people, people_variations, people_contacts, acquisition_sources,
#      items_people_sources, items_makers, source_details
#  - flags duplicates on normalized form of name
# duplicates
#  - reports duplicate people

module Mimsy
  module People
    extend self

    def reshape_contacts
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

        filename = "#{DATADIR}/working/people_contacts.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        post_process do
          puts "File generated in #{filename}"
        end
      end
      Kiba.run(contacts_job)
    end

    def all_people
      reshape_contacts unless File.file?("#{DATADIR}/working/people_contacts.tsv")
      
      people_job = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @deduper = {}

        @varnames = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/people_variations.tsv",
                                             csvopt: TSVOPT,
                                             keycolumn: :link_id)
        @contacts = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/people_contacts.tsv",
                                             csvopt: TSVOPT,
                                             keycolumn: :link_id)
        @acqsrc = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/acquisition_sources.tsv",
                                           csvopt: TSVOPT,
                                           keycolumn: :link_id)
        @ipsrc = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/items_people_sources.tsv",
                                          csvopt: TSVOPT,
                                          keycolumn: :link_id)
        @imsrc = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/items_makers.tsv",
                                          csvopt: TSVOPT,
                                          keycolumn: :link_id)
        @srcdeets = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/source_details.tsv",
                                             csvopt: TSVOPT,
                                             keycolumn: :link_id)
        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/mimsy/people.tsv",
          csv_options: TSVOPT
        transform { |r| r.to_h }

        # This is a weird, seemingly invalid record
        transform FilterRows::FieldEqualTo, action: :reject, field: :link_id, value: '-9999'

        transform Copy::Field, from: :preferred_name, to: :termDisplayName
        transform Clean::RegexpFindReplaceFieldVals,
          fields: [:termDisplayName],
          find: '\\\\$',
          replace: ''

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
          conditions: {
            exclude: {
              field_equal: { fieldsets: [
                {
                  type: :any,
                  matches: [
                    ['row::termDisplayName', 'mergerow::variation']
                  ]
                }
              ]}
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

        # ------------------------------
        # MERGE FROM ACQUISITION_SOURCES
        # ------------------------------
        transform Merge::MultiRowLookup,
          lookup: @acqsrc,
          keycolumn: :link_id,
          fieldmap: {
            :ac_variant => :source
          },
          constantmap: {
            :ac_termPrefForLangNonPreferred => 'false',
            :ac_termSourceLocalNonPreferred => 'Mimsy export',
            :ac_termSourceDetailNonPreferred => 'acquisition_sources.tsv/SOURCE'
          },
          conditions: {
            exclude: {
              field_empty: { fieldsets: [
                {
                  fields: ['mergerow::source']
                }
              ]}
            }
          },
          delim: MVDELIM

        transform Merge::MultiRowLookup,
          lookup: @acqsrc,
          keycolumn: :link_id,
          fieldmap: {
            :ac_occupation => :source_role
          },
          delim: MVDELIM

        transform Merge::MultiRowLookup,
          lookup: @acqsrc,
          keycolumn: :link_id,
          fieldmap: {
            :ac_contactName => :contact
          },
          delim: MVDELIM

        # ------------------------------
        # MERGE FROM ITEMS_PEOPLE_SOURCES
        # ------------------------------
        transform Merge::MultiRowLookup,
          lookup: @ipsrc,
          keycolumn: :link_id,
          fieldmap: {
            :ip_variant => :source
          },
          constantmap: {
            :ip_termPrefForLangNonPreferred => 'false',
            :ip_termSourceLocalNonPreferred => 'Mimsy export',
            :ip_termSourceDetailNonPreferred => 'items_people_sources.tsv/SOURCE'
          },
          conditions: {
            exclude: {
              field_empty: { fieldsets: [
                {
                  fields: ['mergerow::source']
                }
              ]}
            }
          },
          delim: MVDELIM

        transform Merge::MultiRowLookup,
          lookup: @ipsrc,
          keycolumn: :link_id,
          fieldmap: {
            :ip_occupation => :source_role
          },
          delim: MVDELIM

        transform Merge::MultiRowLookup,
          lookup: @ipsrc,
          keycolumn: :link_id,
          fieldmap: {
            :ip_contactName => :contact
          },
          delim: MVDELIM

        # ------------------------------
        # MERGE FROM ITEMS_MAKERS
        # ------------------------------
        transform Merge::MultiRowLookup,
          lookup: @imsrc,
          keycolumn: :link_id,
          fieldmap: {
            :im_variant => :name
          },
          constantmap: {
            :im_termPrefForLangNonPreferred => 'false',
            :im_termSourceLocalNonPreferred => 'Mimsy export',
            :im_termSourceDetailNonPreferred => 'items_makers.tsv/NAME'
          },
          conditions: {
            exclude: {
              field_empty: { fieldsets: [
                {
                  fields: ['mergerow::name']
                }
              ]}
            }
          },
          delim: MVDELIM

        transform Merge::MultiRowLookup,
          lookup: @imsrc,
          keycolumn: :link_id,
          fieldmap: {
            :im_relationship => :relationship
          },
          delim: MVDELIM

        transform Merge::MultiRowLookup,
          lookup: @imsrc,
          keycolumn: :link_id,
          fieldmap: {
            :im_affiliation => :affiliation
          },
          delim: MVDELIM

        transform Merge::MultiRowLookup,
          lookup: @imsrc,
          keycolumn: :link_id,
          fieldmap: {
            :im_bio => :brief_bio
          },
          delim: MVDELIM

        # ------------------------------
        # MERGE FROM ITEMS_MAKERS
        # ------------------------------
        transform Merge::MultiRowLookup,
          lookup: @srcdeets,
          keycolumn: :link_id,
          fieldmap: {
            :sd_variant => :variation
          },
          constantmap: {
            :sd_var_termPrefForLangNonPreferred => 'false',
            :sd_var_termSourceLocalNonPreferred => 'Mimsy export',
            :sd_var_termSourceDetailNonPreferred => 'source_details.tsv/VARIATION'
          },
          conditions: {
            exclude: {
              field_empty: { fieldsets: [
                {
                  fields: ['mergerow::variation']
                }
              ]}
            }
          },
          delim: MVDELIM

        transform Merge::MultiRowLookup,
          lookup: @srcdeets,
          keycolumn: :link_id,
          fieldmap: {
            :sd_pref => :preferred_name
          },
          constantmap: {
            :sd_pref_termPrefForLangNonPreferred => 'false',
            :sd_pref_termSourceLocalNonPreferred => 'Mimsy export',
            :sd_pref_termSourceDetailNonPreferred => 'source_details.tsv/PREFERRED_NAME'
          },
          conditions: {
            exclude: {
              field_empty: { fieldsets: [
                {
                  fields: ['mergerow::preferred_name']
                }
              ]}
            }
          },
          delim: MVDELIM

        # birth_date, birth_place, death_date, death_place, and nationality do not add any new data
        transform Merge::MultiRowLookup,
          lookup: @srcdeets,
          keycolumn: :link_id,
          fieldmap: {
            :occupation => :occupation
          },
          delim: MVDELIM
        
        transform Merge::MultiRowLookup,
          lookup: @srcdeets,
          keycolumn: :link_id,
          fieldmap: {
            :sd_bio => :brief_bio
          },
          delim: MVDELIM

        # omitting merging address since it is very sparsely populated and not formatted in such a way
        #  that we could easily compare to values from people_contacts

        ## SECTION below combines and cleans up person/org category data from multiple tables
        transform CombineValues::FromFieldsWithDelimiter,
          sources: [:ac_occupation, :ip_occupation, :im_relationship, :im_affiliation],
          target: :group,
          sep: MVDELIM

        transform Clean::DowncaseFieldValues,
          fields: [:group]

        ['donors$', 'do or'].each do |find|
          transform Clean::RegexpFindReplaceFieldVals,
            fields: [:group],
            find: find,
            replace: 'donor',
            multival: true,
            sep: MVDELIM
        end
        
        transform Deduplicate::FieldValues,
          fields: [:group],
          sep: MVDELIM

        transform Delete::EmptyFieldValues, fields: [:group], sep: MVDELIM
        ## END SECTION

        ## SECTION below merges in unique :brief_bio from other sources
        transform CombineValues::FromFieldsWithDelimiter,
          sources: [:brief_bio, :im_bio, :sd_bio],
          target: :brief_bio,
          sep: MVDELIM

        transform Deduplicate::FieldValues,
          fields: [:brief_bio],
          sep: MVDELIM
        ## END SECTION

        ## SECTION below merges unique contact name info from other tables
        transform CombineValues::FromFieldsWithDelimiter,
          sources: [:contactName, :ac_contactName, :ip_contactName],
          target: :contactName,
          sep: MVDELIM

        transform Delete::EmptyFieldValues, fields: [:contactName], sep: MVDELIM

        transform Deduplicate::FieldValues,
          fields: [:contactName],
          sep: MVDELIM

        transform Delete::FieldValueIfEqualsOtherField,
          delete: :contactName,
          if_equal_to: :termDisplayName,
          multival: true,
          sep: MVDELIM,
          case_sensitive: false
        ## END SECTION
        
        ## SECTION below merges in variant name info from other tables and cleans it up
        transform CombineValues::FromFieldsWithDelimiter,
          sources: [:termDisplayNameNonPreferred, :ac_variant, :ip_variant, :im_variant, :sd_variant, :sd_pref],
          target: :termDisplayNameNonPreferred,
          sep: MVDELIM

        transform CombineValues::FromFieldsWithDelimiter,
          sources: [:termPrefForLangNonPreferred, :ac_termPrefForLangNonPreferred, :ip_termPrefForLangNonPreferred, :im_termPrefForLangNonPreferred, :sd_var_termPrefForLangNonPreferred, :sd_pref_termPrefForLangNonPreferred],
          target: :termPrefForLangNonPreferred,
          sep: MVDELIM

        transform CombineValues::FromFieldsWithDelimiter,
          sources: [:termSourceLocalNonPreferred, :ac_termSourceLocalNonPreferred, :ip_termSourceLocalNonPreferred, :im_termSourceLocalNonPreferred, :sd_var_termSourceLocalNonPreferred, :sd_pref_termSourceLocalNonPreferred],
          target: :termSourceLocalNonPreferred,
          sep: MVDELIM

        transform CombineValues::FromFieldsWithDelimiter,
          sources: [:termSourceDetailNonPreferred, :ac_termSourceDetailNonPreferred, :ip_termSourceDetailNonPreferred, :im_termSourceDetailNonPreferred, :sd_var_termSourceDetailNonPreferred, :sd_pref_termSourceDetailNonPreferred],
          target: :termSourceDetailNonPreferred,
          sep: MVDELIM

        transform Delete::FieldValueIfEqualsOtherField,
          delete: :termDisplayNameNonPreferred,
          grouped_fields: [:termPrefForLangNonPreferred, :termSourceLocalNonPreferred, :termSourceDetailNonPreferred],
          if_equal_to: :termDisplayName,
          multival: true,
          sep: MVDELIM,
          case_sensitive: false

        transform Deduplicate::GroupedFieldValues,
          on_field: :termDisplayNameNonPreferred,
          grouped_fields: [:termPrefForLangNonPreferred, :termSourceLocalNonPreferred, :termSourceDetailNonPreferred],
          sep: MVDELIM
        ## END SECTION

        transform Deduplicate::FieldValues,
          fields: [:occupation],
          sep: MVDELIM

        ## SECTION below does some cleanup on various columns to make values more consistent
        transform Clean::RegexpFindReplaceFieldVals,
          fields: [:addressStateOrProvince],
          find: 'Georgia',
          replace: 'GA'

        transform Clean::RegexpFindReplaceFieldVals,
          fields: [:nationality],
          find: 'USA',
          replace: 'American'
        
        ## END SECTION
        transform Clean::DelimiterOnlyFields, delim: MVDELIM 
        
        ## SECTION below adds column to flag duplicate terms
        transform Cspace::ConvertToID, source: :termDisplayName, target: :shortid
        transform Deduplicate::Flag, on_field: :shortid, in_field: :duplicate, using: @deduper
        ## END SECTION

        #  show_me!
        

        filename = "#{DATADIR}/working/people.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          initial_headers: [:termDisplayName],
          csv_options: TSVOPT

        post_process do
          puts "File generated in #{filename}"
        end
      end
      Kiba.run(people_job)
    end

    def duplicates
      all_people unless File.file?("#{DATADIR}/working/people.tsv")
      
      duplicate_job = Kiba.parse do
        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/people.tsv", csv_options: TSVOPT
        # Ruby's CSV gives us "CSV::Row" but we want Hash
        transform { |r| r.to_h }

        transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate, value: 'y'

        transform Delete::Fields, fields: [:duplicate]

        filename = 'data/reports/DUPLICATE_people.tsv'
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        post_process do
          puts "File generated in #{filename}"
        end
      end
      Kiba.run(duplicate_job)
    end
  end
end
