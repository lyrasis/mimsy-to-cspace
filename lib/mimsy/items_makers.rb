# frozen_string_literal: true

# new_names
#  - reports (to screen only) whether this table contains any names not present in people.csv
# for_merge
#  - creates working copy of items_makers with preferred_name & individual columns merged
#    in from people, role column inserted based on relationship, affiliation, and
#    prior attribution

module Mimsy
  module ItemsMakers
    extend self

    def new_names
      items_makers_chk_job = Kiba.parse do
        pre_process do
          @rowct = 0
        end
        
        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/items_makers.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }

        @people = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/people.tsv",
                                           csvopt: TSVOPT,
                                           keycolumn: :link_id)

        transform Merge::MultiRowLookup,
          fieldmap: {:p_preferred_name => :preferred_name},
          lookup: @people,
          keycolumn: :link_id,
          delim: MVDELIM

        transform FilterRows::FieldPopulated,
          action: :reject,
          field: :p_preferred_name
        ## END SECTION

        transform do |row|
          @rowct += 1
          row
        end
        
        extend Kiba::Common::DSLExtensions::ShowMe
        show_me!

        post_process do
          if @rowct == 0
            puts 'No names in items_makers that are not in people'
          else
            puts "#{@rowct} names in items_makers that are not in people."
          end
        end
      end
      Kiba.run(items_makers_chk_job)
    end
    
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
