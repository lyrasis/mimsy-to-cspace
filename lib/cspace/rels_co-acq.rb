require_relative 'config'
require_relative 'prelim_cat'

Mimsy::Cat.setup

# creates table of acquisitions keys


# creates table of acquisition items keys

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

  @aikeys = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/acqitem_link.tsv",
                                      csvopt: TSVOPT,
                                      keycolumn: :m_id)
  @acqkeys = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/acq_link.tsv",
                                      csvopt: TSVOPT,
                                      keycolumn: :akey)

  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/catalogue.tsv", csv_options: TSVOPT
  transform { |r| r.to_h }
  transform{ |r| @srcrows += 1; r }
  
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
