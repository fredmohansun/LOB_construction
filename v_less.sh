view_less () {
    echo "Start processing $0 ..."
    sed -i '' -E 's/^[^,]*,(([^,]*,){3})[^,]*,(([^,]*,){5}).*$/\1\3/' $0
    sed -i '' -E 's/^([^,]*,)([0-9]*(-|D)){3}(([0-9]*(:|\.)){3})([0-9]{4})([0-9]){5}/\1\4\7/' $0
    sed -i '' -E 's/^(([^,]*,){6})([A-Z0-9]){12}(([A-Z0-9]){4}),(([0-9]){4}),$/\1\4,\6/' $0
    sed -i '' -E '1 s/,$//' $0
    echo "Process for $0 has been done!"
}

export -f view_less

find . -regex '.*view_less/.*\.csv' -exec sh -c 'view_less "$0"' {} \;

unset -f view_less
