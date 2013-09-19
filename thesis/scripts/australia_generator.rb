count = ARGV[0]

# Australia Bounds
#[113, -43, 153, -10]

count.to_i.times do
    lng = rand(113.0..153.0)
    lat = rand(-43.0..-10.0)
    puts "#{lng},#{lat}"
end
