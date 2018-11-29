
package mlbpipe

import (
    "fmt"
    "log"
    "os"
    "net/http"
    "regexp"
    "path"
    "io"
    "io/ioutil"
    "strings"
)

type xmlResult struct {
    url string
    err error
    statusCode int
    bytes int64
    filename string
}

const MLBBaseURL string = "http://gd2.mlb.com/components/game/mlb/"
const DayFormat string = "year_%d/month_%02d/day_%02d"
const GameIdPattern string = "day_[0-3][0-9]/"+"gid_[0-9]{4}_[0-9]{2}_[0-9]{2}_[a-z]{6}_[a-z]{6}_[0-9]*"

// visit the gameday url and send the game id link along for further processing
func GetGames( gameday string, games chan string) {
    res, err := http.Get(gameday)
    if err != nil {
        log.Fatal(err)
    }
    if res.StatusCode < 200 || res.StatusCode >= 300 {
        fmt.Fprintf(os.Stderr, "expected 200, got: %d from: %s\n", gameday)
        os.Exit(1)
    }

    htmlbuf, err := ioutil.ReadAll(res.Body)

    // find all game links
    re,_ := regexp.Compile(GameIdPattern)
    hrefs := re.FindAll(htmlbuf, -1)

    for _, href := range hrefs {
        games <- path.Join(gameday, string(href[7:]))
    }
    close(games)
}

func savexml(xmlUrl string, status chan xmlResult, isInning bool, optPath string) {
    result := xmlResult{}
    result.url = xmlUrl

    res, err := http.Get(xmlUrl)
    if err != nil {
        log.Fatal(err)
    }

    // exit early if the response is not ok
    if res.StatusCode < 200 || res.StatusCode >= 300 {
        result.statusCode = res.StatusCode
        status<-result
        return
    }

    // get the indices to create the filename from the url 
    start := strings.Index(xmlUrl, "gid")
    var end int
    if isInning {
        end = strings.Index(xmlUrl, "/inning")
    } else {
        end = strings.LastIndex(xmlUrl, "/")
    }

    // create the path name from the gid in the url
    dirname := xmlUrl[start:end]
    dirname = path.Join(optPath, dirname)
    err = os.MkdirAll(dirname, 0700)
    if os.IsExist(err) {
        log.Println("directory exists", dirname)
    }

    // create the file and write the xml in the response body to the file
    filename := path.Join(dirname, path.Base(xmlUrl))
    f, err := os.Create(filename)
    defer f.Close()
    var byteswritten int64
    if err != nil {
        result.err = err
    } else {
        byteswritten, err = io.Copy(f, res.Body)
        if err != nil {
            result.err = err
        }
    }

    result.statusCode = res.StatusCode
    result.bytes = byteswritten
    result.filename = filename
    status<-result
}

// one goroutine per xml resource
func DelegateXMLWork(games chan string, optPath string) {
    logbad  := log.New(os.Stderr, "[XML PROCESSING ERROR] ", log.Ldate | log.Ltime)
    loggood := log.New(os.Stdout, "[SUCCESS] ", log.Ldate | log.Ltime)

    done := make(chan xmlResult)

    // endpoints for each game 
    method := "http://"
    inning := "/inning/inning_all.xml"
    game := "/game.xml"
    players := "/players.xml"

    // we visit 3 endpoints per game
    endpointsToVisit := 0
    for href := range games {
        endpointsToVisit+=3
        go savexml(method + path.Join(string(href[6:]), game), done, false, optPath)
        go savexml(method + path.Join(string(href[6:]), players), done, false, optPath)
        go savexml(method + path.Join(string(href[6:]), inning), done, true, optPath)
    }

    // let us know how the process went
    for i:=0; i < endpointsToVisit; i++ {
        status := <-done
        if status.err != nil {
            logbad.Printf("-- (%d) %s url: %s", status.statusCode, status.err, status.url)
        } else if status.statusCode >= 300 || status.statusCode < 200 {
            logbad.Printf("-- (%d) %s %s", status.statusCode, "could not retrieve data from host: ", status.url )
        } else {
            loggood.Printf("-- (%d) %d bytes written to %s", status.statusCode, status.bytes, status.filename)
        }
    }
    close(done)
}
