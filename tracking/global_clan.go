package tracking

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var (
	load    = godotenv.Load()
	baseURL = os.Getenv("PROXY_URL")
)

func createMongoClient(uri string) *mongo.Client {
	// Uses the SetServerAPIOptions() method to set the Stable API version to 1
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	// Defines the options for the MongoDB client
	opts := options.Client().ApplyURI(uri).SetServerAPIOptions(serverAPI).SetCompressors([]string{"snappy"})
	// Creates a new client and connects to the server
	client, err := mongo.Connect(opts)
	if err != nil {
		panic(err)
	}

	// Sends a ping to confirm a successful connection
	var result bson.M
	if err := client.Database("admin").RunCommand(context.TODO(), bson.D{{"ping", 1}}).Decode(&result); err != nil {
		panic(err)
	}
	fmt.Println("Pinged your deployment. You successfully connected to MongoDB!")
	return client
}

type ClanTracking struct {
	statsMongoClient  *mongo.Client
	staticMongoClient *mongo.Client
	baseUrl           string
	tags              []string

	httpClient *http.Client
	batchSize  int

	results chan Result

	priority Priority

	apiPauseMu   sync.Mutex
	apiPauseCond *sync.Cond
	apiPaused    bool
}

func (c *ClanTracking) waitForAPI() {
	c.apiPauseMu.Lock()
	for c.apiPaused {
		c.apiPauseCond.Wait()
	}
	c.apiPauseMu.Unlock()
}

func (c *ClanTracking) pauseAPI() {
	c.apiPauseMu.Lock()
	c.apiPaused = true
	c.apiPauseMu.Unlock()
}

func (c *ClanTracking) checkAPIHealth() bool {
	reqURL := c.baseUrl + "/goldpass/seasons/current"
	res, err := c.httpClient.Get(reqURL)
	if err != nil {
		return false
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		_, _ = io.ReadAll(res.Body)
		return false
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return false
	}
	season := goldpassSeasonCurrent{}
	if err := json.Unmarshal(body, &season); err != nil {
		return false
	}
	return season.StartTime != "" && season.EndTime != ""
}

func (c *ClanTracking) startAPIHealthMonitor() {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			healthy := c.checkAPIHealth()
			c.apiPauseMu.Lock()
			if healthy {
				if c.apiPaused {
					c.apiPaused = false
					c.apiPauseCond.Broadcast()
				}
			} else {
				fmt.Println("API health check failed, pausing API")
				c.apiPaused = true
			}
			c.apiPauseMu.Unlock()
			<-ticker.C
		}
	}()
}

type Result struct {
	OldClan    *Clan
	OldRecords *Records
	NewClan    *Clan
}

type IconUrls struct {
	Small string `bson:"small" json:"small"`
}

type BadgeUrls struct {
	Small string `bson:"small" json:"small"`
}

type APIRef struct {
	ID   int    `bson:"id" json:"id"`
	Name string `bson:"name" json:"name"`
}

type LeagueTier struct {
	ID       int      `bson:"id" json:"id"`
	Name     string   `bson:"name" json:"name"`
	IconUrls IconUrls `bson:"iconUrls" json:"iconUrls"`
}

type Location struct {
	ID          int    `bson:"id" json:"id"`
	Name        string `bson:"name" json:"name"`
	IsCountry   bool   `bson:"isCountry" json:"isCountry"`
	CountryCode string `bson:"countryCode" json:"countryCode"`
}

type ClanMember struct {
	Tag                 string     `bson:"tag" json:"tag"`
	Name                string     `bson:"name" json:"name"`
	Role                string     `bson:"role" json:"role"`
	TownHallLevel       int        `bson:"townHallLevel" json:"townHallLevel"`
	ExpLevel            int        `bson:"expLevel" json:"expLevel"`
	LeagueTier          LeagueTier `bson:"leagueTier" json:"leagueTier"`
	Trophies            int        `bson:"trophies" json:"trophies"`
	BuilderBaseTrophies int        `bson:"builderBaseTrophies" json:"builderBaseTrophies"`
	ClanRank            int        `bson:"clanRank" json:"clanRank"`
	PreviousClanRank    int        `bson:"previousClanRank" json:"previousClanRank"`
	Donations           int        `bson:"donations" json:"donations"`
	DonationsReceived   int        `bson:"donationsReceived" json:"donationsReceived"`
	BuilderBaseLeague   APIRef     `bson:"builderBaseLeague" json:"builderBaseLeague"`
}

type Label struct {
	ID       int      `bson:"id" json:"id"`
	Name     string   `bson:"name" json:"name"`
	IconUrls IconUrls `bson:"iconUrls" json:"iconUrls"`
}

type CapitalDistrict struct {
	ID                int    `bson:"id" json:"id"`
	Name              string `bson:"name" json:"name"`
	DistrictHallLevel int    `bson:"districtHallLevel" json:"districtHallLevel"`
}

type ClanCapital struct {
	CapitalHallLevel int               `bson:"capitalHallLevel" json:"capitalHallLevel"`
	Districts        []CapitalDistrict `bson:"districts" json:"districts"`
}

type ChatLanguage struct {
	ID           int    `bson:"id" json:"id"`
	Name         string `bson:"name" json:"name"`
	LanguageCode string `bson:"languageCode" json:"languageCode"`
}

type Clan struct {
	Tag                         string       `bson:"tag" json:"tag"`
	Name                        string       `bson:"name" json:"name"`
	Type                        string       `bson:"type" json:"type"`
	Description                 string       `bson:"description" json:"description"`
	Location                    Location     `bson:"location" json:"location"`
	IsFamilyFriendly            bool         `bson:"isFamilyFriendly" json:"isFamilyFriendly"`
	BadgeUrls                   BadgeUrls    `bson:"badgeUrls" json:"badgeUrls"`
	Level                       int          `bson:"clanLevel" json:"clanLevel"`
	ClanPoints                  int          `bson:"clanPoints" json:"clanPoints"`
	ClanBuilderBasePoints       int          `bson:"clanBuilderBasePoints" json:"clanBuilderBasePoints"`
	ClanCapitalPoints           int          `bson:"clanCapitalPoints" json:"clanCapitalPoints"`
	CapitalLeague               APIRef       `bson:"capitalLeague" json:"capitalLeague"`
	RequiredTrophies            int          `bson:"requiredTrophies" json:"requiredTrophies"`
	WarFrequency                string       `bson:"warFrequency" json:"warFrequency"`
	WarWinStreak                int          `bson:"warWinStreak" json:"warWinStreak"`
	WarWins                     int          `bson:"warWins" json:"warWins"`
	WarTies                     int          `bson:"warTies" json:"warTies"`
	WarLosses                   int          `bson:"warLosses" json:"warLosses"`
	IsWarLogPublic              bool         `bson:"isWarLogPublic" json:"isWarLogPublic"`
	WarLeague                   APIRef       `bson:"warLeague" json:"warLeague"`
	Members                     int          `bson:"members" json:"members"`
	MemberList                  []ClanMember `bson:"memberList" json:"memberList"`
	Labels                      []Label      `bson:"labels" json:"labels"`
	RequiredBuilderBaseTrophies int          `bson:"requiredBuilderBaseTrophies" json:"requiredBuilderBaseTrophies"`
	RequiredTownhallLevel       int          `bson:"requiredTownhallLevel" json:"requiredTownhallLevel"`
	ClanCapital                 ClanCapital  `bson:"clanCapital" json:"clanCapital"`
	ChatLanguage                ChatLanguage `bson:"chatLanguage" json:"chatLanguage"`
}

type Record struct {
	Value int `bson:"value"`
	Time  int `bson:"time"`
}

type Records struct {
	ClanPoints   Record `bson:"clanPoints"`
	WarWinStreak Record `bson:"warWinStreak"`
}

type ClanDoc struct {
	Data    Clan    `bson:"data"`
	Records Records `bson:"records"`
}

type tagGroup struct {
	ID string `bson:"_id"`
}

type Priority struct {
	Clans   map[string]bool
	Players map[string]bool
}

type goldpassSeasonCurrent struct {
	StartTime string `json:"startTime"`
	EndTime   string `json:"endTime"`
}

func (c *ClanTracking) priorityClansAndPlayers() Priority {
	coll := c.staticMongoClient.Database("usafam").Collection("clans")

	var clanTags []string
	_ = coll.Distinct(context.TODO(), "tag", bson.D{}).Decode(&clanTags)
	priorityClans := map[string]bool{}
	for _, clanTag := range clanTags {
		priorityClans[clanTag] = true
	}

	coll = c.staticMongoClient.Database("usafam").Collection("user_settings")
	var playerTags []string
	_ = coll.Distinct(context.TODO(), "search.player.bookmarked", bson.D{}).Decode(&playerTags)
	priorityPlayers := map[string]bool{}
	for _, playerTag := range playerTags {
		priorityPlayers[playerTag] = true
	}

	return Priority{
		Clans:   priorityClans,
		Players: priorityPlayers,
	}
}

func clanDiff(oldClan, newClan Clan) bson.M {
	var toSet bson.M

	t := reflect.TypeOf(newClan)
	ov := reflect.ValueOf(oldClan)
	nv := reflect.ValueOf(newClan)

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)

		bsonTag := f.Tag.Get("bson")
		if bsonTag == "" || bsonTag == "-" {
			continue
		}

		key, _, _ := strings.Cut(bsonTag, ",")
		if key == "" || key == "-" {
			continue
		}

		of := ov.Field(i)
		nf := nv.Field(i)

		changed := false
		if f.Type.Comparable() {
			changed = !of.Equal(nf)
		} else {
			changed = !reflect.DeepEqual(of.Interface(), nf.Interface())
		}

		if changed {
			if toSet == nil {
				toSet = bson.M{}
			}
			toSet["data."+key] = nf.Interface()
		}
	}

	return toSet
}

func (c *ClanTracking) getClans(tags []string) {

	limiter := time.NewTicker(time.Second / 1000)
	defer limiter.Stop()

	coll := c.statsMongoClient.Database("looper").Collection("all_clans")
	cursor, err := coll.Find(context.TODO(), bson.D{{"tag", bson.D{{"$in", tags}}}})

	c.priority = c.priorityClansAndPlayers()

	if err != nil {
		fmt.Println(err)
	}
	defer func() {
		_ = cursor.Close(context.TODO())
	}()

	sem := make(chan struct{}, 50)

	getClan := func(tag string) Clan {
		c.waitForAPI()
		<-limiter.C
		escapedTag := url.QueryEscape(tag)
		reqURL := c.baseUrl + "/clans/" + escapedTag
		res, err := c.httpClient.Get(reqURL)
		if err != nil {
			fmt.Println(err)
			return Clan{}
		}
		defer func() {
			_ = res.Body.Close()
		}()
		clan := Clan{}
		body, err := io.ReadAll(res.Body)
		if err != nil {
			fmt.Println(err)
			return Clan{}
		}
		err = json.Unmarshal(body, &clan)
		if err != nil {
			fmt.Println(err, body)
			return Clan{}
		}
		return clan
	}

	found := make(map[string]bool, len(tags))

	var wg sync.WaitGroup
	for cursor.Next(context.TODO()) {
		doc := new(ClanDoc)
		if err := cursor.Decode(doc); err != nil {
			continue
		}
		tag := doc.Data.Tag
		found[tag] = true
		docCopy := doc

		sem <- struct{}{}
		wg.Add(1)
		go func(d Clan, r Records, t string) {
			defer wg.Done()

			defer func() { <-sem }() // release

			newClan := getClan(t)

			c.results <- Result{
				OldClan:    &d,
				OldRecords: &r,
				NewClan:    &newClan,
			}
		}(docCopy.Data, docCopy.Records, tag)
	}

	for _, tag := range tags {
		if _, ok := found[tag]; ok {
			continue
		}

		tagCopy := tag

		sem <- struct{}{}
		wg.Add(1)
		go func(t string) {
			defer wg.Done()
			defer func() { <-sem }()

			newClan := getClan(t)

			c.results <- Result{
				OldClan:    &Clan{},
				OldRecords: nil,
				NewClan:    &newClan,
			}
		}(tagCopy)
	}
	wg.Wait()
	return
}

func (c *ClanTracking) handleResults() {
	db := c.statsMongoClient.Database("looper")
	clans := db.Collection("all_clans")
	clanChanges := db.Collection("all_clans_changes")
	joinLeave := db.Collection("join_leave_history")
	playerStats := db.Collection("player_stats")

	const maxBatch = 500

	clansWrite := make([]mongo.WriteModel, 0, 750)
	clanChangesWrite := make([]mongo.WriteModel, 0, 750)
	joinLeaveWrite := make([]mongo.WriteModel, 0, 750)
	playerStatsWrite := make([]mongo.WriteModel, 0, 750)

	// do real season logic
	season := time.Now().UTC().Format("2006-01")
	priorityClans := map[string]struct{}{}
	priorityPlayers := map[string]struct{}{}

	flush := func() {
		var err error
		if len(clansWrite) >= maxBatch {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			_, err = clans.BulkWrite(ctx, clansWrite, options.BulkWrite().SetOrdered(false))
			cancel()
			clansWrite = clansWrite[:0]
		}
		if len(clanChangesWrite) >= maxBatch {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			_, err = clanChanges.BulkWrite(ctx, clanChangesWrite, options.BulkWrite().SetOrdered(false))
			cancel()
			clanChangesWrite = clanChangesWrite[:0]
		}
		if len(joinLeaveWrite) >= maxBatch {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			_, err = joinLeave.BulkWrite(ctx, joinLeaveWrite, options.BulkWrite().SetOrdered(false))
			cancel()
			joinLeaveWrite = joinLeaveWrite[:0]
		}
		if len(playerStatsWrite) >= maxBatch {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			_, err = playerStats.BulkWrite(ctx, playerStatsWrite, options.BulkWrite().SetOrdered(false))
			cancel()
			playerStatsWrite = playerStatsWrite[:0]
		}
		if err != nil {
			fmt.Println("mongo bulkwrite error:", err)
		}

	}

	for result := range c.results {

		if result.NewClan == nil || result.NewClan.Tag == "" {
			continue
		}

		nowUnix := int(time.Now().UTC().Unix())

		if result.OldClan != nil && result.OldClan.Tag != "" {
			clanTag := result.NewClan.Tag
			currentMembers := make(map[string]ClanMember, len(result.NewClan.MemberList))
			previousMembers := make(map[string]ClanMember, len(result.OldClan.MemberList))
			for _, m := range result.NewClan.MemberList {
				currentMembers[m.Tag] = m
			}
			for _, m := range result.OldClan.MemberList {
				previousMembers[m.Tag] = m
			}

			for tag, member := range currentMembers {
				if _, ok := previousMembers[tag]; ok {
					continue
				}
				doc := bson.M{
					"type": "join",
					"clan": clanTag,
					"time": time.Now().UTC(),
					"tag":  tag,
					"name": member.Name,
					"th":   member.TownHallLevel,
				}
				joinLeaveWrite = append(joinLeaveWrite, mongo.NewInsertOneModel().SetDocument(doc))
			}

			for tag, member := range previousMembers {
				if _, ok := currentMembers[tag]; ok {
					continue
				}
				doc := bson.M{
					"type": "leave",
					"clan": clanTag,
					"time": time.Now().UTC(),
					"tag":  tag,
					"name": member.Name,
					"th":   member.TownHallLevel,
				}
				joinLeaveWrite = append(joinLeaveWrite, mongo.NewInsertOneModel().SetDocument(doc))
			}

			if result.OldClan.Description != result.NewClan.Description {
				doc := bson.M{
					"type":     "description",
					"clan":     clanTag,
					"previous": result.OldClan.Description,
					"current":  result.NewClan.Description,
					"time":     nowUnix,
				}
				clanChangesWrite = append(clanChangesWrite, mongo.NewInsertOneModel().SetDocument(doc))
			}

			if result.OldClan.Level != result.NewClan.Level {
				doc := bson.M{
					"type":     "clan_level",
					"clan":     clanTag,
					"previous": result.OldClan.Level,
					"current":  result.NewClan.Level,
					"time":     nowUnix,
				}
				clanChangesWrite = append(clanChangesWrite, mongo.NewInsertOneModel().SetDocument(doc))
			}

			if _, ok := priorityClans[clanTag]; !ok {
				for tag, curr := range currentMembers {
					if _, ok := priorityPlayers[tag]; ok {
						continue
					}
					prev := previousMembers[tag]
					donationChange := curr.Donations - prev.Donations
					receivedChange := curr.DonationsReceived - prev.DonationsReceived
					if donationChange > 0 || receivedChange > 0 {
						filter := bson.M{"tag": tag, "season": season, "clan_tag": clanTag}
						update := bson.M{"$inc": bson.M{"donations": donationChange, "donationsReceived": receivedChange}}
						playerStatsWrite = append(playerStatsWrite, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true))
					}
				}
			}
		}

		filter := bson.M{"tag": result.NewClan.Tag}
		setOnInsert := bson.M{
			"active":  true,
			"records": bson.M{},
		}

		// New clan or missing old state -> write full data on insert/upsert
		if result.OldClan == nil || result.OldClan.Tag == "" {
			update := bson.M{
				"$set": bson.M{
					"tag":  result.NewClan.Tag,
					"data": result.NewClan,
				},
				"$setOnInsert": setOnInsert,
			}
			clansWrite = append(clansWrite, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true))
			continue
		}

		// Existing clan -> only $set changed fields under data.*
		toSet := clanDiff(*result.OldClan, *result.NewClan)

		if result.OldRecords != nil {
			if result.NewClan.WarWinStreak > result.OldRecords.WarWinStreak.Value {
				toSet["records.warWinStreak"] = Record{Value: result.NewClan.WarWinStreak, Time: nowUnix}
			}
			if result.NewClan.ClanPoints > result.OldRecords.ClanPoints.Value {
				toSet["records.clanPoints"] = Record{Value: result.NewClan.ClanPoints, Time: nowUnix}
			}
		}

		if len(toSet) == 0 {
			continue
		}
		update := bson.M{
			"$set": toSet,
		}
		clansWrite = append(clansWrite, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true))
		flush()
	}
	flush()
}

func (c *ClanTracking) start() {
	go c.handleResults()
	batchTags := make([]string, 0, c.batchSize)
	index := 0

	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{}}},
		{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$tag"}}}},
	}
	coll := c.statsMongoClient.Database("looper").Collection("all_clans")
	for {
		time.Sleep(2 * time.Second)

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		cur, err := coll.Aggregate(ctx, pipeline)
		cancel()
		if err != nil {
			continue
		}
		batchNum := 0

		runBatch := func() {
			batchNum++
			started := time.Now()
			c.getClans(batchTags)
			dur := time.Since(started)
			fmt.Printf("batch=%d size=%d took=%s rate=%.1f tags/s\n",
				batchNum, len(batchTags), dur, float64(len(batchTags))/dur.Seconds(),
			)
			batchTags = batchTags[:0]
		}

		runCtx := context.Background()
		for cur.Next(runCtx) {
			var row tagGroup
			if err := cur.Decode(&row); err != nil {
				fmt.Println(err)
				continue
			}
			batchTags = append(batchTags, row.ID)
			index++

			if len(batchTags) >= c.batchSize {
				runBatch()
			}
		}

		if len(batchTags) > 0 {
			runBatch()
		}
		fmt.Println("num tags processed:", index)
		index = 0

		_ = cur.Close(runCtx)
		if err := cur.Err(); err != nil {
			fmt.Println("cursor err:", err)
		}
	}
}

func RunGlobalClan() {

	tracking := ClanTracking{
		statsMongoClient:  createMongoClient(os.Getenv("STATS_MONGODB_URI")),
		staticMongoClient: createMongoClient(os.Getenv("STATIC_MONGODB_URI")),
		baseUrl:           baseURL,
		batchSize:         5000,
		httpClient: &http.Client{
			Timeout: 20 * time.Second,
			Transport: &http.Transport{
				MaxConnsPerHost:     1000,
				MaxIdleConns:        200,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,

				TLSHandshakeTimeout:   10 * time.Second,
				ResponseHeaderTimeout: 10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,

				ForceAttemptHTTP2: true,
			},
		},
		results:  make(chan Result, 500),
		priority: Priority{},
	}
	tracking.apiPauseCond = sync.NewCond(&tracking.apiPauseMu)
	tracking.apiPaused = true
	tracking.startAPIHealthMonitor()

	tracking.start()
}
