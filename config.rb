# frozen_string_literal: true

require 'fileutils'

require 'kiba'
require 'kiba-common/sources/csv'
require 'kiba-common/destinations/csv'
require 'kiba-common/dsl_extensions/show_me'
require 'kiba/extend'
require 'pry'
require 'facets/kernel/blank'

require_relative 'lib/mimsy'
require_relative 'lib/cspace'

# :test or :full -- which set of collectionobjects will be used as base
MODE = :full
TSVOPT = {headers: true, col_sep: "\t", header_converters: :symbol, converters: [:stripplus]}
LOCCSVOPT = {headers: true, header_converters: :symbol, converters: [:stripplus]}
MVDELIM = ';'
DATADIR = File.expand_path('~/code/mimsy-to-cspace/data')

# SECTION BELOW moves previous working files to backup directory
timestamp = Time.now.strftime("%y-%m-%d_%H-%M")
backupdir = "#{DATADIR}/backup"
workingdir = "#{DATADIR}/working"

FileUtils.cd(workingdir) do
Dir.each_child(workingdir) do |filename|
  new_name = "#{timestamp}_#{filename}"
  FileUtils.mv(filename, "#{backupdir}/#{new_name}")
  end
end
# END SECTION

LANGUAGES = {
  'eng' => 'English'
}
GENDER = {
  'F' => 'female',
  'M' => 'male',
  'N' => nil
}
INVSTATUS = {
  'ACCESSION' => 'unprocessed',
  'ACCESSION DETAIL' => 'processed'
}
PRIORATTR = {
  'N' => '',
  'Y' => ' (prior attribution)'
}
PUBLISH = {
  'Y' => 'All',
  'N' => 'None'
}
REPRO = {
  'Y' => 'Reproduction allowed.',
  'N' => 'Reproduction not allowed.'
}
MEDIATYPE = {
  'IMAGE' => 'still_image',
  'DOCUMENT' => 'document'
}

