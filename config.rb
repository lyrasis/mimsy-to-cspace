require 'kiba'
require 'kiba-common/sources/csv'
require 'kiba-common/destinations/csv'
require 'kiba-common/dsl_extensions/show_me'
require 'pry'
require 'facets/kernel/blank'

TSVOPT = {headers: true, col_sep: "\t", header_converters: :symbol, converters: [:stripplus]}
CSVOUTOPT = {converters: [:cleardelimonly]}
MVDELIM = ';'
LANGUAGES = {
  'eng' => 'English'
}
GENDER = {
  'F' => 'female',
  'M' => 'male',
  'N' => nil
}

# strips, collapses multiple spaces, removes terminal commas, strips again
CSV::Converters[:stripplus] = lambda{ |s|
  begin
    if s.nil?
      nil
    else
      s.strip
       .gsub(/  +/, ' ')
       .sub(/,$/, '')
       .strip
    end
  rescue ArgumentError
    s
  end
}

CSV::Converters[:cleardelimonly] = lambda{ |s|
  begin
    if s.nil?
      nil
    else
      s.gsub(MVDELIM, '').empty? ? nil : s.gsub(MVDELIM, '')
    end
  rescue ArgumentError
    s
  end
}

module Clean
  class FieldClean
    def initialize(fields:, instructions:)
      @fields = fields
      @instructions = instructions
    end

    def process(row)
      @fields.each do |f|
        fval = row.fetch(f)
        unless fval.nil?
          fval = do_replaces(fval, @instructions.fetch(:replace, {}))
          fval = conditional_remove(fval, @instructions.fetch(:remove_if_contains, []))
          fval = fval.empty? ? nil : fval
        end
        row[f] = fval
      end
      row
    end

    private

    def conditional_remove(val, arr)
      arr.each{ |m| val = '' if val.match?(Regexp.new(m)) }
      val
    end
    
    def do_replaces(val, hash)
      hash.each do |find, replace|
        val = val.gsub(Regexp.new(find), replace)
      end
      val
    end
  end
end

module Lookup
  module_function

  # use when keycolumn values are unique
  # creates hash with keycolumn value as key and csv-row-as-hash as the value
  def csv_to_simple_hash(file:, keycolumn:)
    CSV.foreach(file, TSVOPT).each_with_object({}) do |r, memo|
      memo[r.fetch(keycolumn)] = r.to_h
    end
  end

  # use when keycolumn values are not unique
  # creates hash with keycolumn value as key and array of csv-rows-as-hashes as the value
  def csv_to_multi_hash(file:, keycolumn:)
    CSV.foreach(file, TSVOPT).each_with_object({}) do |r, memo|
      k = r.fetch(keycolumn)
      if memo.has_key?(k)
        memo[k] << r.to_h
      else
        memo[k] = [r.to_h]
      end
    end
  end

  # used when lookup may return an array of rows from which values should be merged
  #  into the target
  class MultiRowLookupMerge
    def initialize(fieldmap:, constantmap: {}, lookup:, keycolumn:, exclusion_criteria: {})
      @fieldmap = fieldmap # hash of looked-up values to merge in for each merged-in row
      @constantmap = constantmap #hash of constants to add for each merged-in row
      @lookup = lookup #lookuphash; should be created with csv_to_multi_hash
      @keycolumn = keycolumn #column in main table containing value expected to be lookup key
      @exclusion_criteria = exclusion_criteria #hash of constraints a row must NOT meet in order to be merged 
    end

    def process(row)
      id = row.fetch(@keycolumn)
      h = {}
      @fieldmap.each_key{ |k| h[k] = [] }
      @constantmap.each_key{ |k| h[k] = [] }
      
      @lookup.fetch(id, []).each do |mrow|
        unless exclude?(row, mrow)
          @fieldmap.each{ |target, source| h[target] << mrow.fetch(source, '') }
          @constantmap.each{ |target, value| h[target] << value }
        end
      end

      chk = @fieldmap.map{ |target, source| h[target].size }.uniq.sort

      if chk[0] == 0
        h.each{ |target, arr| row[target] = nil }
      else
        h.each{ |target, arr| row[target] = arr.join(MVDELIM) }
      end
      
      row
    end

    private

    def exclude?(row, mrow)
      bool = [false]
      @exclusion_criteria.each do |type, hash|
        case type
        when :equal
          bool << exclude_on_equality?(row, mrow, hash)
        end
      end
      bool.flatten.any? ? true : false
    end

    def exclude_on_equality?(row, mrow, hash)
      bool = []
      hash.each{ |rowfield, mergefield| row.fetch(rowfield) == mrow.fetch(mergefield) ? bool << true : bool << false }
      bool
    end
  end
end

class ConstantValue
  def initialize(target:, value:)
    @target = target
    @value = value
  end

  def process(row)
    row[@target] = @value
    row
  end
end

# mapping should be one of the hashes defined as a constant up at the top
class StaticFieldValueMapping
  def initialize(source:, target:, mapping:, delete_source: true)
    @source = source
    @target = target
    @mapping = mapping
    @del = delete_source
  end

  def process(row)
    row[@target] = @mapping[row.fetch(@source)] unless row.fetch(@source).blank?
    row.delete(@source) if @del
    row
  end
end

module SelectRows
  class WithFieldEqualTo
    def initialize(action:, field:, value:)
      @column = field
      @value = value
      @action = action
    end

    def process(row)
      case @action
        when :keep
          row.fetch(@column) == @value ? row : nil
      when :reject
        row.fetch(@column) == @value ? nil : row
      end
    end
  end
end

module Reshape
  # can take multiple fields like :workphone and :homephone
  #   and produce two new fields like :phone and :phonetype
  #   where :phonetype depends on the original field taken from
  class CombineAndTypeFields
    def initialize(sourcefieldmap:, datafield:, typefield:, default_type: '', delete_sources: true)
      @map = sourcefieldmap
      @df = datafield
      @tf = typefield
      @default_type = default_type
      @del = delete_sources
    end

    def process(row)
      data = []
      type = []
      @map.keys.each do |sf|
        val = row.fetch(sf)
        unless val.nil?
          data << val
          type << @map.fetch(sf, @default_type)
        end
        row.delete(sf) if @del
      end
      row[@df] = data.size > 0 ? data.join(MVDELIM) : nil
      row[@tf] = type.size > 0 ? type.join(MVDELIM) : nil
      row
    end
  end
end

class RenameField
  def initialize(from:, to:)
    @from = from
    @to = to
  end
  
  def process(row)
    row[@to] = row.fetch(@from)
    row.delete(@from)
    row
  end
end

class DeleteFields
  def initialize(fields)
    @fields = fields
  end

  def process(row)
    @fields.each{ |f| row.delete(f) }
    row
  end
end

# map the value of one source field to two or more target fields
class OneColumnToMulti
  def initialize(source:, targets:, delete_source: true)
    @source = source
    @targets = targets
    @del = delete_source
  end

  def process(row)
    sourceval = row.fetch(@source)
    @targets.each{ |target| row[target] = sourceval }
    row.delete(@source) if @del
    row
  end
end

# concatenate the values of two columns with a separator if both are populated
class ConcatColumns
  def initialize(sources:, target:, sep:, delete_sources: true)
    @sources = sources
    @target = target
    @sep = sep
    @del = delete_sources
  end

  def process(row)
    val = @sources.map{ |src| row.fetch(src) }.compact.join(@sep)
    val.empty? ? row[@target] = nil : row[@target] = val
    @sources.each{ |src| row.delete(src) } if @del
    row
  end
end

# concatenate another source column's value to a single target field value
# expected use is for non-repeatable fields
class AppendStringToFieldValue
  def initialize(target_column:, source_column:, sep:, delete_source: true)
    @target = target_column
    @source = source_column
    @sep = sep
    @del = delete_source
  end

  def process(row)
    if row.fetch(@source).blank?
      # do nothing
    elsif row[@target].blank?
      row[@target] = row.fetch(@source)
    else
      row[@target] = row.fetch(@target) + @sep + row.fetch(@source)
    end
    row.delete(@source) if @del
    row
  end
end
