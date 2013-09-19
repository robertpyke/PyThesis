count = ARGV[0]

# Brisbane Bounds
# [151.766402, -28.141693, 153.831832, -26.855379]

count.to_i.times do
    lng = rand(151.766402..153.831832)
    lat = rand(-28.141693..-26.855379)
    puts "#{lng},#{lat}"
end
