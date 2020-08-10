require_relative 'config'

module Mimsy
  module Inscription
    def self.setup
      @typeclean = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0
        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/inscriptions.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[inscription_type],
          find: ' in,',
          replace: ' ink,',
          casesensitive: false
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[inscription_type],
          find: 'i6nk| inkn',
          replace: 'ink',
          casesensitive: false
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[inscription_type],
          find: ' in$',
          replace: ' ink',
          casesensitive: false
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[inscription_type],
          find: '^clth',
          replace: 'cloth',
          casesensitive: false
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[inscription_type],
          find: 'stapel',
          replace: 'staple',
          casesensitive: false
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[inscription_type],
          find: 'lable|labe3l',
          replace: 'label',
          casesensitive: false
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[inscription_type],
          find: 'emboidered',
          replace: 'embroidered',
          casesensitive: false
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[inscription_type],
          find: '`$',
          replace: '',
          casesensitive: false
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[inscription_type],
          find: 'hadnwrit',
          replace: 'handwrit',
          casesensitive: false
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[inscription_type],
          find: 'driting|dritten|drwiting|dwitten|dwreitten|dwritig',
          replace: 'dwritten',
          casesensitive: false
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[inscription_type],
          find: 'hand ?(w?ritingt?|writring|printed)',
          replace: 'handwritten',
          casesensitive: false
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[inscription_type],
          find: 'hand writing',
          replace: 'handwritten',
          casesensitive: false
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[inscription_type],
          find: 'bgack',
          replace: 'back',
          casesensitive: false
        
        #  show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/inscriptiontype_clean.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        post_process do
          puts "\n\nINSCRIPTION TYPE BASIC CLEAN"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end

      @inscrip = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0
        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/inscriptiontype_clean.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform Rename::Field, from: :inscription_text, to: :inscriptionContent
        transform Rename::Field, from: :original_language, to: :inscriptionContentLanguage
        transform Rename::Field, from: :translation, to: :inscriptionContentTranslation
        
        transform do |row|
          val = row.fetch(:inscription_type, nil)
          if val
            val = val.downcase
            if val['stamp'] and !val['stamped']
              type = 'stamp'
            elsif val['cloth label']
              type = 'cloth-label'
            elsif val['label']
              type = 'label'
            elsif val['hallmark']
              type = 'hallmark'
            elsif val['makers mark']
              type = "maker's-mark"
            elsif val['tag']
              type = 'paper-tag'
            elsif val['signature']
              type = 'signature'
            elsif val['sticker']
              type = 'label'
            else
              type = nil
            end
            row[:inscriptionContentType] = type
          else
            row[:inscriptionContentType] = nil
          end
          row
        end

        transform do |row|
          val = row.fetch(:inscription_type, nil)
          if val
            val = val.downcase
            if val['emboss']
              method = 'embossed'
            elsif val['embroid']
              method = 'embroidered'
            elsif val['ink']
              method = 'pen-and-ink'
            elsif val['maarker']
              method = 'pen-and-ink'
            elsif val['pencil']
              method = 'pencil'
            elsif val['handwritten']
              method = 'handwritten-unspecified'
            elsif val['stamp']
              method = 'stamped'
            elsif val['engrav']
              method = 'engraved'
            elsif val['etch']
              method = 'etched'
            elsif val['paint']
              method = 'painted'
            elsif val['stencil']
              method = 'stenciled'
            elsif val['typ']
              method = 'typed'
            elsif val['sewn']
              method = 'embroidered'
            elsif val['pen']
              method = 'pen-and-ink'
            else
              method = nil
            end
            row[:inscriptionContentMethod] = method
          else
            row[:inscriptionContentMethod] = nil
          end
          row
        end

        transform do |row|
          val = row.fetch(:inscription_type, nil)
          if val
            val = val.downcase
            if val['hebrew']
              row[:inscriptionContentLanguage] = 'Hebrew'
            elsif val['german']
              row[:inscriptionContentLanguage] = 'German'
            end
          end
          row
        end

        transform do |row|
          val = row.fetch(:inscription_location, nil)
          if val
            testval = val.downcase
            if testval['back']
              pos = 'back'
            elsif testval['base']
              pos = 'base'
            elsif testval['bottom']
              pos = 'bottom'
            elsif testval['front']
              pos = 'front'
            elsif testval['inside']
              pos = 'inside'
            elsif testval['recto']
              pos = 'recto'
            elsif testval['verso']
              pos = 'verso'
            elsif testval['top']
              pos = 'top'
            elsif testval['face']
              pos = 'front'
            else
              pos = nil
            end
            row[:inscriptionContentPosition] = pos
          else
            row[:inscriptionContentPosition] = nil
          end
          row
        end

        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[inscription_type inscription_location],
          target: :inscriptionContentInterpretation,
          sep: ' --- ',
          prepend_source_field_name: true

        #  show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/inscription.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        post_process do
          puts "\n\nINSCRIPTION DATA TO MERGE"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end

      Kiba.run(@typeclean)
      Kiba.run(@inscrip)
    end
  end
end
