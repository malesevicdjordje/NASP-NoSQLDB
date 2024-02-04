package main

import (
	engine "KVSystem/system"
	"KVSystem/system/structures"
	"bufio"
	"fmt"
	"os"
	"strings"
)

func menu() {
	fmt.Println("\n======= MENU =======")
	fmt.Println(" 1. PUT")
	fmt.Println(" 2. GET")
	fmt.Println(" 3. DELETE")
	fmt.Println(" 4. EDIT")
	fmt.Println("--- HyperLogLog  ---")
	fmt.Println(" 5. CREATE HLL")
	fmt.Println(" 6. ADD TO HLL")
	fmt.Println(" 7. ESTIMATE HLL")
	fmt.Println("-- CountMinSketch --")
	fmt.Println(" 8. CREATE CMS")
	fmt.Println(" 9. ADD TO CMS")
	fmt.Println("10. QUERY IN CMS")
	fmt.Println("--------------------")
	fmt.Println("0. EXIT")
	fmt.Print("\nChose option from menu: ")
}

func scan() string {
	input := bufio.NewScanner(os.Stdin)
	input.Scan()
	return input.Text()
}

func request(engine *engine.Engine) bool {
	request := engine.TokenBucket.AllowRequest()
	if !request {
		fmt.Println("Too much requests ! Please try again later !")
		return false
	}
	return true
}

func parseChoice(choice string, engine *engine.Engine) bool {
	switch choice {
	case "0":
		engine.Wal.PersistCurrentSegment()
		fmt.Println("\nGoodbye !")
		return false
	case "1":
		if !request(engine) {
			break
		}
		fmt.Println("\n- PUT")
		fmt.Print("Key: ")
		key := scan()
		fmt.Print("Value: ")
		value := scan()
		if engine.Put(key, []byte(value), false) {
			fmt.Println("Data inserted !")
		} else {
			fmt.Println("Could not put data !")
		}
		break
	case "2":
		if !request(engine) {
			break
		}
		fmt.Println("\n- GET")
		fmt.Print("Key: ")
		key := scan()
		value := engine.GetAsString(key)
		fmt.Println("Value: ", value)
		break
	case "3":
		if !request(engine) {
			break
		}
		fmt.Println("\n- DELETE")
		fmt.Print("Key: ")
		key := scan()
		if engine.Delete(key) {
			fmt.Println("Data deleted !")
		} else {
			fmt.Println("Could not find data to delete!")
		}
		break
	case "4":
		if !request(engine) {
			break
		}
		fmt.Println("\n- EDIT")
		fmt.Print("Key: ")
		key := scan()
		fmt.Print("Value: ")
		value := scan()
		if engine.Edit(key, []byte(value)) {
			fmt.Println("Value edited !")
		} else {
			fmt.Println("Could not find data to edit !")
		}
		break
	case "5":
		if !request(engine) {
			break
		}
		fmt.Println("\n- CREATE HLL")
		fmt.Print("HLL's Key: ")
		key := "hll-" + scan()
		value := structures.CreateHyperLogLog(uint8(engine.Config.HLLParameters.HLLPrecision)).SerializeHLL()
		if engine.Put(key, value, false) {
			fmt.Println("HLL created !")
		}
		break
	case "6":
		if !request(engine) {
			break
		}
		fmt.Println("\n- ADD TO HLL")
		fmt.Print("HLL's Key: ")
		key := "hll-" + scan()
		ok, hllData := engine.Get(key)
		if !ok {
			fmt.Println("HLL with given key not found !")
			break
		}
		hll := structures.DeserializeHLL(hllData)
		fmt.Print("Value to add: ")
		value := scan()
		hll.Add(value)
		if engine.Edit(key, hll.SerializeHLL()) {
			fmt.Println("Value added !")
		} else {
			fmt.Println("Could not add data !")
		}
		break
	case "7":
		if !request(engine) {
			break
		}
		fmt.Println("\n- ESTIMATE HLL")
		fmt.Print("HLL's Key: ")
		key := "hll-" + scan()
		ok, hllData := engine.Get(key)
		if !ok {
			fmt.Println("HLL with given key not found !")
			break
		}
		hll := structures.DeserializeHLL(hllData)
		fmt.Println("Estimation: ", hll.Evaluate())
		break
	case "8":
		if !request(engine) {
			break
		}
		fmt.Println("\n- CREATE CMS")
		fmt.Print("CMS's Key: ")
		key := "cms-" + scan()
		value := structures.CreateCountMinSketch(0.1, 0.01).SerializeCMS()
		if engine.Put(key, value, false) {
			fmt.Println("CMS created !")
		}
		break
	case "9":
		if !request(engine) {
			break
		}
		fmt.Println("\n- ADD TO CMS")
		fmt.Print("CMS's Key: ")
		key := "cms-" + scan()
		ok, cmsData := engine.Get(key)
		if !ok {
			fmt.Println("CMS with given key not found !")
			break
		}
		cms := structures.DeserializeCMS(cmsData)
		fmt.Print("Value to add: ")
		value := scan()
		cms.Add(strings.ToUpper(value))
		if engine.Edit(key, cms.SerializeCMS()) {
			fmt.Println("Value added !")
		} else {
			fmt.Println("Could not add data !")
		}
		break
	case "10":
		if !request(engine) {
			break
		}
		fmt.Println("\n- QUERY IN CMS")
		fmt.Print("CMS's Key: ")
		key := "cms-" + scan()
		ok, cmsData := engine.Get(key)
		if !ok {
			fmt.Println("CMS with given key not found !")
			break
		}
		fmt.Print("Value to query: ")
		value := scan()
		cms := structures.DeserializeCMS(cmsData)
		fmt.Println(value, " ? : ", cms.Search(strings.ToUpper(value)))
		break
	default:
		fmt.Println("\nWrong input ! Please try again. ")
		break
	}

	return true
}

func main() {
	system := new(engine.Engine)
	system.Init()
	fmt.Println("Welcome !")
	run := true
	for run {
		menu()
		run = parseChoice(scan(), system)
	}
}
