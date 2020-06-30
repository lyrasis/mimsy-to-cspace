require_relative 'config'

mediajob = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe
  @srcrows = 0
  @outrows = 0

    @cat = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/catalogue.tsv",
                                       csvopt: TSVOPT,
                                       keycolumn: :mkey)
    @med = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/media.tsv",
                                       csvopt: TSVOPT,
                                       keycolumn: :mediakey)

  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/items_media.tsv", csv_options: TSVOPT
  # Ruby's CSV gives us "CSV::Row" but we want Hash
  transform { |r| r.to_h }
  transform{ |r| @srcrows += 1; r }
  
  transform Delete::FieldsExcept, keepfields: %i[mkey mediakey media]

  transform Merge::MultiRowLookup,
    lookup: @cat,
    keycolumn: :mkey,
    fieldmap: {
      :object_id => :id_number
    }
  
  transform Merge::MultiRowLookup,
    lookup: @med,
    keycolumn: :mediakey,
    fieldmap: {
      :filename => :media_id
    }

  transform Merge::MultiRowLookup,
    lookup: @med,
    keycolumn: :mediakey,
    fieldmap: {
      rectype: :record_type
    }
  transform Replace::FieldValueWithStaticMapping,
    source: :rectype,
    target: :type,
    mapping: MEDIATYPE,
    fallback_val: nil


  transform Merge::MultiRowLookup,
    lookup: @med,
    keycolumn: :mediakey,
    fieldmap: {
      repro_allowed: :repro_allowed
    }
  transform Replace::FieldValueWithStaticMapping,
    source: :repro_allowed,
    target: :repro,
    mapping: REPRO,
    fallback_val: nil
  
  transform Merge::MultiRowLookup,
    lookup: @med,
    keycolumn: :mediakey,
    fieldmap: {
      publishto: :publish
    }
  transform Replace::FieldValueWithStaticMapping,
    source: :publishto,
    target: :publishTo,
    mapping: PUBLISH,
    fallback_val: nil

  transform Merge::ConstantValue, target: :copyright, value: 'Copyright restrictions may apply. Permission to publish or reproduce must be secured from the repository and the copyright holder.'

  transform CombineValues::FromFieldsWithDelimiter,
    sources: %i[copyright repro],
    target: :copyrightStatement,
    sep: ' '

  transform do |row|
    filename = row.fetch(:filename, nil)
    if filename
      row[:blob_uri] = "https://path/to/aws_bucket/#{filename}"
    else
      row[:blob_uri] = nil
    end
    row
  end
    

  transform Delete::FieldValueIfEqualsOtherField,
    delete: :media,
    if_equal_to: :filename

  transform Delete::Fields, fields: %i[media]

    #   show_me!
    transform{ |r| @outrows += 1; r }
  filename = "#{DATADIR}/working/media_handling.tsv"
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
    csv_options: TSVOPT
  post_process do
    puts "\n\nMEDIA HANDLING DATA"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end
Kiba.run(mediajob)

mediaprocjob = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe
  @srcrows = 0
  @outrows = 0

  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/media_handling.tsv", csv_options: TSVOPT
  # Ruby's CSV gives us "CSV::Row" but we want Hash
  transform { |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

  transform FilterRows::FieldPopulated, action: :keep, field: :object_id
  transform Delete::Fields, fields: %i[mkey mediakey object_id]
  transform Rename::Field, from: :filename, to: :identificationNumber
    #   show_me!
    transform{ |r| @outrows += 1; r }
  filename = "#{DATADIR}/cs/media_handling.csv"
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
    initial_headers: %i[identificationNumber],
    csv_options: CSVOPT
  post_process do
    puts "\n\nMEDIA HANDLING PROCEDURES"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end
Kiba.run(mediaprocjob)

mediareljob = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe
  @srcrows = 0
  @outrows = 0

  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/media_handling.tsv", csv_options: TSVOPT
  # Ruby's CSV gives us "CSV::Row" but we want Hash
  transform { |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

  transform FilterRows::FieldPopulated, action: :keep, field: :object_id
  transform Delete::FieldsExcept, keepfields: %i[object_id filename]
  transform Rename::Field, from: :filename, to: :objectIdentifier
  transform Merge::ConstantValue, target: :objectDocumentType, value: 'MediaHandling'
  transform Rename::Field, from: :object_id, to: :subjectIdentifier
  transform Merge::ConstantValue, target: :subjectDocumentType, value: 'CollectionObject'

  #   show_me!
    transform{ |r| @outrows += 1; r }
  filename = "#{DATADIR}/cs/rels_co-mh.csv"
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
    csv_options: CSVOPT
  post_process do
    puts "\n\nOBJECT-MEDIA HANDLING RELATIONSHIPS"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end
Kiba.run(mediareljob)

mediaomitjob = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe
  @srcrows = 0
  @outrows = 0

  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/media_handling.tsv", csv_options: TSVOPT
  # Ruby's CSV gives us "CSV::Row" but we want Hash
  transform { |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

  transform FilterRows::FieldPopulated, action: :reject, field: :object_id
    #   show_me!
    transform{ |r| @outrows += 1; r }
  filename = "#{DATADIR}/reports/media_handling_with_no_object_relationship.csv"
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
    csv_options: CSVOPT
  post_process do
    puts "\n\nMEDIA HANDLING WITH NO OBJECT"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end
Kiba.run(mediaomitjob)
