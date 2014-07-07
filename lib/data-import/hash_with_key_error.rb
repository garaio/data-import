module HashWithKeyError
  def [](key)
    if !has_key?(key)
      raise "key '#{key}' not in hash #{self}"
    end
    self.fetch(key)
  end
end

