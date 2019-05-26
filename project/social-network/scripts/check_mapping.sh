comm -3 <(sort -f ../log/actual_mappings.txt) <(sort -f ../expected_mappings.txt) > wrong_mappings.txt
