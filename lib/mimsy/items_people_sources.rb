# frozen_string_literal: true

# new_names
#  - reports (to screen only) whether this table contains any names not present in people.csv

module Mimsy
  module ItemsPeopleSources
    extend self

    def new_names
      items_people_src_chk_job = Kiba.parse do
        pre_process do
          @rowct = 0
        end
        
        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/items_people_sources.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }

        @people = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/people.tsv",
                                           csvopt: TSVOPT,
                                           keycolumn: :link_id)

        transform Merge::MultiRowLookup,
          fieldmap: {:p_preferred_name => :preferred_name},
          lookup: @people,
          keycolumn: :link_id,
          delim: MVDELIM

        ## This section reports any names in acquisition_sources.tsv that are not in
        ##  people.csv
        ##  Currently there are none, so we don't need to do anything about this!
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
            puts 'No names in items_people_sources that are not in people'
          else
            puts "#{@rowct} names in items_people_sources that are not in people."
          end
        end
      end
      Kiba.run(items_people_src_chk_job)
    end
  end
end
