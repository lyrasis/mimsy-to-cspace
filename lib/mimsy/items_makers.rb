# frozen_string_literal: true

# for_merge
#  - creates working copy of items_makers with preferred_name & individual columns merged
#    in from people, role column inserted based on relationship, affiliation, and
#    prior attribution

module Mimsy
  module ItemsMakers
    extend self
    def for_merge
      namesjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0
        @names = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/people.tsv",
                                          csvopt: TSVOPT,
                                          keycolumn: :link_id)

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/items_makers.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform Merge::MultiRowLookup,
          lookup: @names,
          keycolumn: :link_id,
          fieldmap: {
            :preferred_name => :preferred_name,
            :individual => :individual
          }

        # where affiliation = Maker, relationship is blank --- collapse into one downcased column
        transform Rename::Field, from: :relationship, to: :role
        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[role affiliation],
          target: :role,
          sep: ''
        transform Clean::DowncaseFieldValues, fields: [:role]

        # turn "maker" into "maker (prior attribution)" if prior_attribution column = Y
        transform Replace::FieldValueWithStaticMapping,
          source: :prior_attribution,
          target: :prior_attribution_mapped,
          mapping: PRIORATTR,
          delete_source: false
        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[role prior_attribution_mapped],
          target: :role,
          sep: ''

        #show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/items_makers.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        post_process do
          puts "\n\nITEMS_MAKERS PREPPED FOR MERGE"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(namesjob)
    end
  end
end
