# frozen_string_literal: true

# csv
#  - get combined subject table and broader subjects into the same table and
#    format for CSpace concept authority import
# hierarchies
#  - creates hierarchical relationships between subject headings for load into
#    CSpace. 

module Cspace
  module Concept
    extend self

    def csv
      Mimsy::Subject.all unless File.file?("#{DATADIR}/working/subjects_all.tsv")
      all_subjects = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0
        @deduper = {}

        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/working/subjects_all.tsv",
          csv_options: TSVOPT
        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[termsourcenote scopenote],
          find: ';',
          replace: ','

        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[termsourcenote scopenote],
          find: 'LINEBREAKWASHERE',
          replace: "\n"
        
        transform Delete::Fields, fields: %i[msub_id broaderterm broadernorm termnorm duplicate]

        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/cs/concepts.csv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: CSVOPT
        
        post_process do
          puts "\n\nALL CSPACE CONCEPTS"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(all_subjects)
    end

    def hierarchies
      Mimsy::Subject.all unless File.file?("#{DATADIR}/working/subjects_all.tsv")
      create_hier = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        @bts = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/subjects_all.tsv",
                                        csvopt: TSVOPT,
                                        keycolumn: :termnorm)

        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/working/subjects_all.tsv",
          csv_options: TSVOPT

        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        # keep only rows with broader terms
        transform FilterRows::FieldPopulated, action: :keep, field: :broaderterm
        
        transform Rename::Field, from: :termdisplayname, to: :narrower
        
        transform Merge::MultiRowLookup,
          lookup: @bts,
          keycolumn: :broadernorm,
          fieldmap: {:broader => :termdisplayname}

        transform Merge::ConstantValue, target: :type, value: 'Concept'
        transform Merge::ConstantValue, target: :subtype, value: 'concept'

        transform Delete::FieldsExcept, keepfields: %i[type subtype broader narrower]

        transform{ |r| @outrows += 1; r }

        filename = "#{DATADIR}/cs/rels_hier_concepts.csv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: CSVOPT
        
        post_process do
          puts "\n\nCONCEPT HIERARCHY"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(create_hier)
    end
  end
end
