# frozen_string_literal: true

# prep
#  - excludes extraneous rows
#  - merges in objectnumber values for both objects from catalogue
#  - drops any rows without a merged-in objectnumber in either subject or object id field (indicating at
#    least one of the involved objects is not in the migration
#  - concatenates subject and object id for deduping, flags and removes duplicates

module Mimsy
  module RelatedItem
    extend self

    def prep
      Mimsy::Cat.setup unless File.file?("#{DATADIR}/working/catalogue.tsv")
      prepjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0
        @deduper = {}

        @cat = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/catalogue.tsv",
                                             csvopt: TSVOPT,
                                             keycolumn: :mkey)


        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/related_items.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        # sysuseonly = 1 is reciprocal expression of preceding sysuseonly = 0 row
        transform FilterRows::FieldEqualTo, action: :reject, field: :sysuseonly, value: '1'

        # blank relationship rows are hierarchical relationships already extracted from catalogue data
        transform FilterRows::FieldEqualTo, action: :keep, field: :relationship, value: 'related accession'

        transform Delete::FieldsExcept, keepfields: %i[mkey related_mkey]

        # merge in objectnumber for subject of relationship
        transform Merge::MultiRowLookup,
          lookup: @cat,
          keycolumn: :mkey,
          fieldmap: {
            :subject_id => :id_number,
          },
          delim: MVDELIM

        # filter out rows with empty value in subject id
        transform FilterRows::FieldPopulated, action: :keep, field: :subject_id
        
        # merge in objectnumber for object of relationship
        transform Merge::MultiRowLookup,
          lookup: @cat,
          keycolumn: :related_mkey,
          fieldmap: {
            :object_id => :id_number,
          },
          delim: MVDELIM

        # filter out rows with empty value in object id
        transform FilterRows::FieldPopulated, action: :keep, field: :object_id

        transform Delete::FieldsExcept, keepfields: %i[subject_id object_id]

        transform CombineValues::FromFieldsWithDelimiter, sources: %i[subject_id object_id],
          target: :concat,
          sep: ' ',
          delete_sources: false

        transform Deduplicate::Flag, on_field: :concat, in_field: :duplicate, using: @deduper

        transform FilterRows::FieldEqualTo, action: :reject, field: :duplicate, value: 'y'

        transform Delete::Fields, fields: %i[duplicate concat]
        #  show_me!

        
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/rels_co-co_non-hier.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        
        post_process do
          puts "\n\nRELATED ITEM DATA PREPPED"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(prepjob)
    end
    
  end
end
