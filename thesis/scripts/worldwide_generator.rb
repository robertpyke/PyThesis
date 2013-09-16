count = ARGV[0]

count.to_i.times do
    lng = rand(-180.0..180.0)
    lat = rand(-90.0..90.0)
    puts "#{lng},#{lat}"
end
