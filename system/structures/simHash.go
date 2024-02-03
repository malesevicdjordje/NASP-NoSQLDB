package structures

import "strings"

func generateWordsStop() map[string]bool {
	wordsStop := []string{"A", "About", "Actually", "Almost", "Also", "Although", "Always", "Am", "An", "And", "Any", "Are",
		"As", "At", "Be", "Became", "Become", "But", "By", "Can", "Could", "Did", "Do", "Does", "Each", "Either", "Else", "For",
		"From", "Had", "Has", "Have", "Hence", "How", "I", "If", "In", "IS", "IT", "ITS", "JUST", "MAY", "MAYBE", "Me", "Might",
		"Mine", "Must", "My", "Neither", "Nor", "Not", "Of", "Oh", "Ok", "When", "Where", "Whereas", "Wherever", "Whenever",
		"Whether", "Which", "While", "Who", "Whom", "Whoever", "Whose", "Why", "Will", "With", "Within", "Without",
		"Would", "Yes", "Yet", "You", "Your"}
	mapWordsStop := make(map[string]bool)
	for _, word := range wordsStop {
		mapWordsStop[strings.ToUpper(word)] = true
	}
	return mapWordsStop
}

type SimHash struct {
	mapWordsStop map[string]bool
}
