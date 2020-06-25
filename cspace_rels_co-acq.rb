require_relative 'config'
require_relative 'prelim_cat_remove_loans'

Mimsy::Cat.setup

# creates table of acquisitions keys
acqkeys = Kiba.parse do
extend Kiba::Common::DSLExtensions::ShowMe
  @deduper = {}
  @srcrows = 0
  @outrows = 0

  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/acquisitions.tsv", csv_options: TSVOPT
  # Ruby's CSV gives us "CSV::Row" but we want Hash
  transform { |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

  # this only processes rows with ref_number
  transform FilterRows::FieldPopulated, action: :keep, field: :ref_number

  # flag duplicates and remove
  transform Deduplicate::Flag, on_field: :ref_number, in_field: :duplicate, using: @deduper
  transform FilterRows::FieldEqualTo, action: :reject, field: :duplicate, value: 'y'

  transform do |row|
    keep = %i[akey ref_number]
    row.keys.each{ |k| row.delete(k) unless keep.include?(k) }
    row
  end
  #show_me!
  
  transform{ |r| @outrows += 1; r }
  filename = "#{DATADIR}/working/acq_link.tsv"
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
    csv_options: TSVOPT
  
  post_process do
    puts "\n\nACQUISITION LINKAGE"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end  
end
Kiba.run(acqkeys)

# creates table of acquisition items keys
acqitemkeys = Kiba.parse do
extend Kiba::Common::DSLExtensions::ShowMe
  @deduper = {}
  @srcrows = 0
  @outrows = 0

  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/acquisition_items.tsv", csv_options: TSVOPT
  # Ruby's CSV gives us "CSV::Row" but we want Hash
  transform { |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

  # this only processes rows with id_number
  transform FilterRows::FieldPopulated, action: :keep, field: :id_number

  transform do |row|
    keep = %i[akey m_id id_number]
    row.keys.each{ |k| row.delete(k) unless keep.include?(k) }
    row
  end
  #show_me!
  
  transform{ |r| @outrows += 1; r }
  filename = "#{DATADIR}/working/acqitem_link.tsv"
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
    csv_options: TSVOPT
  
  post_process do
    puts "\n\nACQUISITION ITEM LINKAGE"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end  
end
Kiba.run(acqitemkeys)

# creates table of AcqItem CollectionObject relationships to Acquisition procedures
acqitem_acq_rel = Kiba.parse do
extend Kiba::Common::DSLExtensions::ShowMe
  @deduper = {}
  @srcrows = 0
  @outrows = 0

  @acqkeys = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/acq_link.tsv",
                                  csvopt: TSVOPT,
                                  keycolumn: :akey)
  
  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/acqitem_link.tsv", csv_options: TSVOPT
  # Ruby's CSV gives us "CSV::Row" but we want Hash
  transform { |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

  # Only process rows we used to create a stub (not cataloged) collectionobject
  transform FilterRows::FieldPopulated, action: :reject, field: :m_id

  transform Merge::MultiRowLookup,
    lookup: @acqkeys,
    keycolumn: :akey,
    fieldmap: {
      objectIdentifier: :ref_number
    },
    constantmap: {
      objectDocumentType: 'Acquisition'
    },
    delim: MVDELIM

  transform Rename::Field, from: :id_number, to: :subjectIdentifier
  transform Merge::ConstantValue, target: :subjectDocumentType, value: 'CollectionObject'

  transform Delete::Fields, fields: %i[akey m_id]
  #show_me!
  
  transform{ |r| @outrows += 1; r }
  filename = "#{DATADIR}/cs/rel_acq-acqco.csv"
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
    csv_options: CSVOPT
  
  post_process do
    puts "\n\nACQITEM OBJECT-TO-ACQUISITION RELATIONSHIPS"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end  
end
Kiba.run(acqitem_acq_rel)

# creates table of catalogue keys
co_acq_rel = Kiba.parse do
extend Kiba::Common::DSLExtensions::ShowMe
  @deduper = {}
  @srcrows = 0
  @outrows = 0

  @test = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/co_select.tsv",
                                   csvopt: TSVOPT,
                                   keycolumn: :mkey)

  @aikeys = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/acqitem_link.tsv",
                                      csvopt: TSVOPT,
                                      keycolumn: :m_id)
  @acqkeys = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/acq_link.tsv",
                                      csvopt: TSVOPT,
                                      keycolumn: :akey)

  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/catalogue.tsv", csv_options: TSVOPT
  transform { |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

  # SECTION BELOW selects only listed rows for testing
  transform Merge::MultiRowLookup,
    lookup: @test,
    keycolumn: :mkey,
    fieldmap: {
      :keep => :mkey
    }
  transform FilterRows::FieldPopulated, action: :keep, field: :keep
  transform Delete::Fields, fields: %i[keep]
  # END SECTION

  transform Delete::FieldsExcept, keepfields: %i[mkey id_number]

  transform Merge::MultiRowLookup,
    lookup: @aikeys,
    keycolumn: :mkey,
    fieldmap: {
      akey: :akey,
    },
    delim: MVDELIM

  transform FilterRows::FieldPopulated, action: :keep, field: :akey

  transform Explode::RowsFromMultivalField, field: :akey, delim: ';'

  transform Merge::MultiRowLookup,
    lookup: @acqkeys,
    keycolumn: :akey,
    fieldmap: {
      objectIdentifier: :ref_number
    },
    constantmap: {
      objectDocumentType: 'Acquisition'
    },
    delim: MVDELIM

  transform Rename::Field, from: :id_number, to: :subjectIdentifier
  transform Merge::ConstantValue, target: :subjectDocumentType, value: 'CollectionObject'

  transform Delete::Fields, fields: %i[mkey akey]
  #show_me!

  transform{ |r| @outrows += 1; r }
  filename = "#{DATADIR}/cs/rel_acq-co.csv"
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
    csv_options: CSVOPT
  
  post_process do
    puts "\n\nCOLLECTIONOBJECT-TO-ACQUISITION RELATIONSHIPS"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end  
end
Kiba.run(co_acq_rel)
