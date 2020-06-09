require_relative 'config'

wrkacq = Kiba.parse do
  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/acquisitions.tsv", csv_options: TSVOPT
  # Ruby's CSV gives us "CSV::Row" but we want Hash
  transform { |r| r.to_h }

  filename = 'data/working/acquisitions.tsv'
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
    csv_options: TSVOPT
  post_process do
    puts "File generated in #{filename}"
  end
end
Kiba.run(wrkacq)

acqjob = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe

  @srcrows = 0
  @outrows = 0


  @acqitems = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/acquisition_items.tsv",
                                       csvopt: TSVOPT,
                                       keycolumn: :akey)
  @acq = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/acquisitions.tsv",
                                       csvopt: TSVOPT,
                                       keycolumn: :akey)

  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/acquisitions.tsv", csv_options: TSVOPT
  # Ruby's CSV gives us "CSV::Row" but we want Hash
  transform { |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

  transform Delete::Fields, fields: %i[status status_date requested_by request_date legal_date legal_date_display]

  transform Merge::MultiRowLookup,
    lookup: @acq,
    keycolumn: :akey,
    fieldmap: {
      :approvalStatus => :status,
      :approvalDate => :status_date
    },
    constantmap: {
      :approvalIndividual => ''
    },
    delim: MVDELIM

  transform Clean::DowncaseFieldValues,
    fields: %i[approvalStatus]
  
  transform Clean::RegexpFindReplaceFieldVals,
    fields: %i[approvalStatus],
    find: '^acknowledges?$',
    replace: 'acknowledged'
  
  transform Merge::MultiRowLookup,
    lookup: @acq,
    keycolumn: :akey,
    fieldmap: {
      :approvalIndividualReq => :requested_by,
      :approvalDateReq => :request_date
    },
    constantmap: {
      :approvalStatusReq => 'requested'
    },
    delim: MVDELIM

  transform Merge::MultiRowLookup,
    lookup: @acq,
    keycolumn: :akey,
    fieldmap: {
      :approvalDateLegal => :legal_date
    },
    constantmap: {
      :approvalStatusLegal => 'legal',
      :approvalIndividualLegal => ''
    },
    delim: MVDELIM

  transform Merge::MultiRowLookup,
    lookup: @acq,
    keycolumn: :akey,
    fieldmap: {
      :approvalDateLegalDisp => :legal_date_display
    },
    constantmap: {
      :approvalStatusLegalDisp => 'legal',
      :approvalIndividualLegalDisp => ''
    },
    delim: MVDELIM


  transform Merge::MultiRowLookup,
    lookup: @acqitems,
    keycolumn: :akey,
    fieldmap: {
      :aiApprovalStatus => :status,
      :aiApprovalDate => :status_date
    },
    constantmap: {
      :aiApprovalIndiv => ''
    },
    delim: MVDELIM

    transform Clean::DowncaseFieldValues,
    fields: %i[aiApprovalStatus]
  
  transform Clean::RegexpFindReplaceFieldVals,
    fields: %i[aiApprovalStatus],
    find: '^\\\\$',
    replace: ''

  transform CombineValues::AcrossFieldGroup,
    fieldmap: {
      :approvalStatus => %i[approvalStatus approvalStatusReq approvalStatusLegal approvalStatusLegalDisp aiApprovalStatus],
      :approvalIndividual => %i[approvalIndividual approvalIndividualReq approvalIndividualLegal approvalIndividualLegalDisp aiApprovalIndiv],
      :approvalDate => %i[approvalDate approvalDateReq approvalDateLegal approvalDateLegalDisp aiApprovalDate]
    },
    sep: MVDELIM

  transform Clean::EmptyFieldGroups,
    groups: [
      %i[approvalStatus approvalIndividual approvalDate]
    ],
    sep: MVDELIM

    transform Merge::MultiRowLookup,
    lookup: @acq,
    keycolumn: :akey,
    fieldmap: {
      :groupPurchasePriceValue => :total_offer_price
    },
    constantmap: {
      :groupPurchasePriceCurrency => 'US Dollar'
    },
    delim: MVDELIM

  transform Merge::MultiRowLookup,
    lookup: @acqitems,
    keycolumn: :akey,
    fieldmap: {
      :aiAcquisitionNote => :note,
      :aiTransferDate => :transfer_date,
      :accessionDateGroup => :accession_date
    },
    delim: MVDELIM

  transform CombineValues::FromFieldsWithDelimiter,
    sources: %i[decision_reason aiAcquisitionNote],
    target: :acquisitionNote,
    sep: MVDELIM

  # SECTION below standardizes :method values
  transform Delete::FieldValueMatchingRegexp,
    fields: [:method],
    match: '^unkown$'

  transform Clean::RegexpFindReplaceFieldVals,
    fields: [:method],
    find: ' - no ack\. necessary',
    replace: ''
  # END SECTION
  
  transform Rename::Field, from: :authorized_by, to: :acquisitionAuthorizer
  transform Rename::Field, from: :date_authorized, to: :acquisitionAuthorizerDate
  transform Rename::Field, from: :method, to: :acquisitionMethod
  transform Rename::Field, from: :reason, to: :acquisitionReason
  transform Rename::Field, from: :ref_number, to: :acquisitionReferenceNumber
  transform Rename::Field, from: :terms, to: :acquisitionProvisos
  transform Rename::Field, from: :total_offer_price, to: :groupPurchasePriceValue
  
  transform Delete::Fields, fields: %i[akey status requested_by request_date legal_date total_requested
                                       external_file aiTransferDate]
#  transform Clean::DelimiterOnlyFields, delim: MVDELIM

  show_me!
  
  transform{ |r| @outrows += 1; r }
  filename = 'data/cs/acquisitions.csv'
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
    initial_headers: %i[acquisitionReferenceNumber],
    csv_options: CSVOPT
    
  post_process do
    puts "\n\nACQ RECORDS"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end

Kiba.run(acqjob)
