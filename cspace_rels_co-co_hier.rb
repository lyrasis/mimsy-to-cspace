require_relative 'config'
require_relative 'prelim_cat_remove_loans'

Mimsy::Cat.setup

# create table just of test records
catjob = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe
  @deduper = {}
  @srcrows = 0
  @outrows = 0

  @test = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/co_select.tsv",
                                  csvopt: TSVOPT,
                                  keycolumn: :mkey)

  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/catalogue.tsv", csv_options: TSVOPT
  # Ruby's CSV gives us "CSV::Row" but we want Hash
  transform { |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

  # # SECTION BELOW selects only listed rows for testing
  # transform Merge::MultiRowLookup,
  #   lookup: @test,
  #   keycolumn: :mkey,
  #   fieldmap: {
  #     :keep => :mkey
  #   }
  # transform FilterRows::FieldPopulated, action: :keep, field: :keep
  # transform Delete::Fields, fields: %i[keep]
  # # END SECTION
 
  #show_me!
  
  transform{ |r| @outrows += 1; r }
  filename = "#{DATADIR}/working/collectionobjects_rel_testset.tsv"
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
   initial_headers: %i[id_number parent_key broader_text whole_part],
    csv_options: TSVOPT
    
  post_process do
    puts "\n\nCOLLECTIONOBJECT TEST RECORDS"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end
Kiba.run(catjob)

relsjob = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe
  @deduper = {}
  @srcrows = 0
  @outrows = 0

  @mkeylkup = {}
  @ids = []
  
  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/collectionobjects_rel_testset.tsv", csv_options: TSVOPT
  # Ruby's CSV gives us "CSV::Row" but we want Hash
  transform { |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

  # set up the job constants we will use to do matching within the table
  transform do |row|
    idnum = row.fetch(:id_number)
    @mkeylkup[row.fetch(:mkey)] = idnum 
    @ids << idnum
    row
  end

  # SECTION BELOW cleans up category1 values
  transform Clean::RegexpFindReplaceFieldVals,
    fields: %i[category1],
    find: 'ACC+ESS+ION',
    replace: 'ACCESSION'

  transform Clean::RegexpFindReplaceFieldVals,
    fields: %i[category1],
    find: 'DETAL|DETAIl',
    replace: 'DETAIL'
  
    transform Merge::ConstantValueConditional,
    fieldmap: {
      objectName: 'Manuscript',
      objectNameLanguage: 'English'
    },
    conditions: {
      include: {
        field_equal: { fieldsets: [
          {
            matches: [
              ['row::category1', 'value::Manuscript']
            ]
          }
        ]}
      }
    }
  transform Delete::FieldValueMatchingRegexp,
    fields: [:category1],
    match: '^Manuscript$'
  transform Delete::FieldValueMatchingRegexp,
    fields: [:category1],
    match: '^Gelatin silver print$'

  transform Merge::ConstantValueConditional,
    fieldmap: {
      category1: 'ACCESSION DETAIL'
    },
    conditions: {
      include: {
        field_equal: { fieldsets: [
          {
            matches: [
              ['row::category1', 'revalue::\d{2}-\d{3}']
            ]
          }
        ]}
      }
    }
  # END SECTION

  transform Delete::FieldsExcept, keepfields: %i[id_number parent_key broader_text whole_part mkey category1]

  # SECTION BELOW populates :parent and rel_confidence columns

  # unambiguous relationship based on parent_key value match
  transform do |row|
    pk = row.fetch(:parent_key, nil)
    if pk
    if @mkeylkup.has_key?(pk)
      row[:parent] = @mkeylkup[pk]
      row[:rel_confidence] = 'high - based on unambiguous parent_key data relationship'
    else
      row[:parent] = nil
      row[:rel_confidence] = "zero - record with parent_key value (#{pk}) not found in data set"
    end
    else
      row[:parent] = nil
      row[:rel_confidence] = nil
    end
    row
  end

  # relationship based on whole_part value equalling id_number of another row
  #   other row is the parent
  #   cannot do this on ACCESSION rows because they often have their child id_number in this field
  #     and we don't want to set the child as the parent!
  transform do |row|
    if row.fetch(:parent, nil).nil? && row.fetch(:category1, '') == 'ACCESSION DETAIL'
      wp = row.fetch(:whole_part, nil)
      if wp == row.fetch(:id_number)
        row[:rel_confidence] = 'zero - ACCESSION DETAIL record, whole_part matches own id_number'
        elsif wp
        if @ids.any?(wp)
          row[:parent] = wp
          row[:rel_confidence] = 'medium-high - ACCESSION DETAIL record, whole_part field value exactly matches another row id_number'
        end
      end
    end
  row
  end

  # relationship based on splitting id_number on spaces, and trying to find an id that matches
  #   successively shorter segments of the id_number value
  transform do |row|
    if row.fetch(:parent, nil).nil? && row.fetch(:rel_confidence, nil).nil? && row.fetch(:category1, '') == 'ACCESSION DETAIL'
      id = row.fetch(:id_number, nil)
        ids = id.split(' ')
        x = ids.size - 1
        if x > 0
          x.times do |e|
            ids.pop
            compare = ids.join(' ')
            if @ids.any?(compare)
              row[:parent] = compare
              row[:rel_confidence] = 'medium - ACCESSION DETAIL record, id_number split on space matches another row id_number'
              next
            end
          end
        end
      end
    row
  end

  # relationship based on splitting id_number on period, and trying to find an id that matches
  #   successively shorter segments of the id_number value
  transform do |row|
    if row.fetch(:parent, nil).nil? && row.fetch(:rel_confidence, nil).nil? && row.fetch(:category1, '') == 'ACCESSION DETAIL'
      id = row.fetch(:id_number, nil)
      ids = id.split('.')
      x = ids.size - 1
      if x > 0
        x.times do |e|
          ids.pop
          compare = ids.join('.')
          if @ids.any?(compare)
            row[:parent] = compare
            row[:rel_confidence] = 'medium - ACCESSION DETAIL record, id_number split on period matches another row id_number'            
            next
          end
        end
      end
    end
    row
  end

  transform do |row|
    if row.fetch(:parent, nil).nil? && row.fetch(:rel_confidence, nil).nil? && row.fetch(:category1, '') == 'ACCESSION DETAIL'
      if row.fetch(:whole_part, nil)
        row[:rel_confidence] = 'zero - ACCESSION DETAIL record has whole/part value, but cannot find or match parent'
      else
        row[:rel_confidence] = 'zero - ACCESSION DETAIL record, no whole/part value, cannot find or match parent'
      end
    end
    row
  end

  transform do |row|
    if row.fetch(:parent, nil).nil? && row.fetch(:rel_confidence, nil).nil? && row.fetch(:category1, '') == 'ACCESSION'
      if row.fetch(:whole_part, nil)
        row[:rel_confidence] = 'zero - ACCESSION record has whole/part value, but cannot set parent relationship from parent'
      else
        row[:rel_confidence] = 'zero - ACCESSION record, no whole/part value, cannot find or match parent'
      end
    end
    row
  end

    transform do |row|
    if row.fetch(:parent, nil).nil? && row.fetch(:rel_confidence, nil).nil?
      if row.fetch(:whole_part, nil)
        row[:rel_confidence] = 'zero - unknown record level has whole/part value'
      else
        row[:rel_confidence] = 'zero - unknown record level, no whole/part value'
      end
    end
    row
    end
    # END SECTION
    transform Rename::Field, from: :parent, to: :parent_id
#  show_me!

    
  transform{ |r| @outrows += 1; r }
  filename = "#{DATADIR}/working/rels_co-co_data.tsv"
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
   initial_headers: %i[id_number parent_id rel_confidence parent_key broader_text whole_part],
    csv_options: TSVOPT
    
  post_process do
    puts "\n\nCOLLECTIONOBJECT RELATIONSHIP BUILDER"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end
Kiba.run(relsjob)

relspopulated = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe
  @deduper = {}
  @srcrows = 0
  @outrows = 0

  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/rels_co-co_data.tsv", csv_options: TSVOPT
  # Ruby's CSV gives us "CSV::Row" but we want Hash
  transform { |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

  transform FilterRows::FieldPopulated, action: :keep, field: :parent_id

  transform Merge::ConstantValue, target: :type, value: 'CollectionObject'
  transform Delete::FieldsExcept, keepfields: %i[type id_number parent_id]
  transform Rename::Field, from: :id_number, to: :narrower
  transform Rename::Field, from: :parent_id, to: :broader
  
  #show_me!
  
  transform{ |r| @outrows += 1; r }
  filename = "#{DATADIR}/cs/rel_co-co.csv"
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
    csv_options: CSVOPT
    
  post_process do
    puts "\n\nCOLLECTIONOBJECT HIERARCHY"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end
Kiba.run(relspopulated)
