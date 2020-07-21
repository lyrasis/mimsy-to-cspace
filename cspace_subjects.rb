require_relative 'config'
require_relative 'prelim_subject'

Mimsy::Subject.setup

# all_subjects -- append main subject table and broader subjects into the same table and
#  format for CSpace import
# create_hier -- creates hierarchical relationships between subject headings for load into
#  CSpace. 
all_subjects = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe
  @srcrows = 0
  @outrows = 0

  source Kiba::Common::Sources::CSV,
    filename: "#{DATADIR}/working/subjects_deduped.tsv",
    csv_options: TSVOPT
  source Kiba::Common::Sources::CSV,
    filename: "#{DATADIR}/working/subjects_broader.tsv",
    csv_options: TSVOPT
  transform{ |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

  transform Delete::Fields, fields: %i[msub_id broaderterm broadernorm termnorm duplicate]
  transform{ |r| @outrows += 1; r }

  filename = "#{DATADIR}/cs/subjects.csv"
  destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: CSVOPT
  
  post_process do
    puts "\n\nALL SUBJECTS"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end

create_hier = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe
  @srcrows = 0
  @outrows = 0

  @bts = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/subjects_broader.tsv",
                                   csvopt: TSVOPT,
                                   keycolumn: :termnorm)

  source Kiba::Common::Sources::CSV,
    filename: "#{DATADIR}/working/subjects_deduped.tsv",
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

  filename = "#{DATADIR}/cs/rels_hier_subjects.csv"
  destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: CSVOPT
  
  post_process do
    puts "\n\nSUBJECT HIERARCHY"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end

Kiba.run(all_subjects)
Kiba.run(create_hier)
