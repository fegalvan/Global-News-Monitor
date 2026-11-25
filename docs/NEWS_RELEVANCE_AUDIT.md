# News relevance audit for the v1.2 preview

**The reported piece is an obituary.** Both GDELT records turn a reference to military service in Vietnam into apparent current activity there. The audit found two problems in GNM itself as well. Some rules assign Cyber and Crisis labels without enough context, and the parser drops fields the event display needs to explain geographic precision.

**Release recommendation.** Before deploying, identify the cards as original GDELT references and address the narrow GNM rules and metadata gaps below. Keep all original records and current analytics available while testing any relevance changes. The audit does not support replacing the current totals with a broadly filtered feed.

## The reported obituary

The publisher’s article says that Gary Sunderman of Cazenovia, New York died on November 22, 2026. It describes his military service in Vietnam and his life after returning. The service is biographical background. The article provides no support for two contemporary diplomatic visits attributed to Vietnam. [Cazenovia Republican](https://www.eaglenewsonline.com/cazenovia_republican/gary-sunderman-77/article_df7eaae5-cc4b-428c-bbd2-58ce730444e5.html) · [Published obituary on WKTV/Legacy](https://www.legacy.com/us/obituaries/wktv/name/gary-sunderman-obituary?id=63055057)

| GDELT event ID | Original CAMEO action | Original action geography | GNM category |
| --- | --- | --- | --- |
| `1334899681` | `042` Make a visit | `VM`, country-level Vietnam reference | Diplomacy, `root_event_code:04` |
| `1334898592` | `043` Host a visit | `VM`, the same Vietnam centroid | Diplomacy, `root_event_code:04` |

Both rows came from the November 25, 03:15 UTC export. Their stored codes, URL, and country agree with that file. GNM translates `VM` to Vietnam and groups the shared URL into one card, which used the latest matching record’s `042` label. The app’s broad mapping of root `04` into Diplomacy made the source error look like a current diplomatic event. [Original Events export](https://storage.googleapis.com/data.gdeltproject.org/gdeltv2/20261125031500.export.CSV.zip) · [Corresponding Mentions export](https://storage.googleapis.com/data.gdeltproject.org/gdeltv2/20261125031500.mentions.CSV.zip) · [GNM category rules](../src/domain/events/categorization.py)

In the 24 hours ending at 03:30 UTC, the two records were **2 of 114,952 globally**, **2 of 440 for Vietnam**, and **2 of 286 for Vietnam Diplomacy**. They were **2 of 9 Vietnam Diplomacy records in that single 15-minute slot**. The article also occupied one of the 12 recent Vietnam cards. Its effect depended on how narrowly someone was looking.

## What I reviewed

I used the isolated local snapshot from [v1.2 validation](V1.2_VALIDATION.md). The comparison frame was the two complete, successive 24-hour periods ending November 24 and 25 at 03:00 UTC. Each had 96 completed exports. Together they contained **223,045 records and 44,615 distinct stored URLs** across 4,888 hostnames and 227 coded event geographies. The recent-card check included two additional slots, with the reported article in the later one. Coded event geography can differ from the country of the publisher or the location of the actual event.

| Sample | Distinct links | Publisher body readable | Clearly off-scope among those read |
| --- | ---: | ---: | ---: |
| Probability sample of links from both complete days | 42 | 35 | 4 |
| Two different-country links per category and period | 28 | 26 | 2 |
| Global and Vietnam recent cards plus six prior US cards | 27 | 23 | 1 |
| Purposeful checks of obituary, sports, entertainment, sponsored and release paths | 15 | 13 | 10 |
| **Total** | **112** | **97** | **17** |

The probability sample sorted exact trimmed stored URLs by SHA-256 with the fixed prefix `gnm-v12-relevance-20261124-v1|`. The balanced sample spans all seven categories and favors new event geographies and hosts. The card checks cover 30 display positions, with three URLs shared between views. The reported link is among the Vietnam cards. The full sample included **94 hostnames and 46 event geographies**. In 103 of the 112 cases the publisher page declared English, and the other nine had no detected language. Event geographies were more diverse than the visible article languages.

I reviewed visible publisher text where available. Eight links provided usable publisher headlines or descriptions without matching body text. Seven were unavailable under the access limits used. The body-only counts exclude these 15 links. In the probability sample, 24 of the 35 readable articles covered substantive public-interest subjects, seven were borderline, and four were clearly off-scope. The balanced, recent-card, and targeted samples answer different questions and should not be pooled to estimate prevalence. This was one reviewer, two full days, and a small sample of articles. The numbers do not estimate a global record error rate.

I judged **article scope separately from record fit**. An important local story can be relevant even if the sampled action, actors, or geography are wrong. In the mixed set of 97 readable links, 28 had a clear upstream action or geography mismatch on the selected record, two had clear GNM override problems, and ten had GNM category labels broader than the content warranted. Sixteen had plausible support for the sampled topic and action context. Forty-one were unresolved at the record level, often because the article discussed several events or the sampled action could have come from another part of the text.

## Examples verified from publisher text

| Publisher and story | Sampled GNM record | What the article reports |
| --- | --- | --- |
| [Printing Impressions, founder obituary](https://www.piworld.com/article/in-memoriam-gary-r-schellerer/) | Vietnam · Politics · Criticize or denounce | An Illinois business founder’s obituary. It gives no support for a current denunciation in Vietnam. |
| [BJPenn, MMA coverage](https://www.bjpenn.com/mma-news/jiri-prochazka/jiri-prochazka-future-questioned-ptsd-kos/) | Qatar · Diplomacy · Make pessimistic comment | A former fighter commenting on another fighter’s career. Qatar is the recent bout location. |
| [Female First, celebrity relationship](http://www.femalefirst.co.uk/celebrity/sarah-paulson-worried-weird-holland-taylor-lived-apart-1454084.html) | United States · Conflict · Fight with small arms | A feature about a couple’s living arrangements. The sampled action is unsupported. |
| [Daily Meal, Aldi shopping guide](https://www.thedailymeal.com/2326874/stocking-stuffers-aldi-december-2026/) | Belgium · Economics · Cooperate economically | A gift-shopping list. The content establishes the shopping genre, with no basis for claiming a paid placement. |
| [Live 5 News, sponsored membership offer](https://www.live5news.com/sponsored/patriots-point/more-than-membership-gift-that-keeps-stories-alive/) | United States · Diplomacy · Make a visit | An explicitly sponsored promotion for a museum membership. |
| [Echo, A13 traffic disruption](https://www.echo-news.co.uk/news/26757211.crash-partially-blocks-a13-london-bound-sadlers-farm/) | Unknown country · Protest · Obstruct passage | A road crash blocked traffic. The matching GDELT mention had confidence 100. |
| [Prokerala, Indian president’s Odisha visit](https://www.prokerala.com/news/articles/a1840498.html) | Indonesia · Protest · Mobilize or increase armed forces | The visit was for the Bali Yatra festival in India. GDELT supplied the armed-forces action and Indonesia geography. GNM’s mapping of military posture root `15` into Protest added a separate label problem. |
| [The Star, Malaysia minimum-wage enforcement](https://www.thestar.com.my/news/nation/2026/11/23/204-employers-found-breaching-rm1700-minimum-wage) | Malaysia · Cyber · `128` Defy norms, law | Labor-law noncompliance fits the generic source action. GNM matched `breach` in the URL and supplied the Cyber label. |
| [Spotlight Nepal, domestic party leadership](https://www.spotlightnepal.com/2026/11/24/nc-leader-gagan-thapa-urges-deuba-maintain-dignity/) | Nepal · Crisis · `025` Appeal to yield | A party leader urged a former leader to preserve his dignity. GNM’s `crisis_weak_humanitarian_code:025` fallback added the humanitarian interpretation. |
| [Prince Albert Daily Herald, First Nation community services](https://paherald.sk.ca/red-pheasant-hosts-fatherhood-workshop-to-support-communitys-fathers-and-children/) | Unknown country · Diplomacy · Host a visit | A First Nation hosted a workshop for fathers. The hosting language is understandable, and the local public-service story is relevant. |

Useful matches included [Bangladeshi garment workers protesting wages](https://bdnews24.com/bangladesh/1ymta2eqyw), [regional fisheries negotiations in Vanuatu](https://pina.com.fj/2026/11/25/vanuatu-to-host-major-pacific-fisheries-meeting/), [a cyberattack that halted local newspaper printing in New Zealand](https://newswire.co.nz/2026/11/cyber-attack-allied-media-greymouth-star-south-island-newspapers/), and [public advocacy on proposed US deepfake legislation by a celebrity](https://www.breitbart.com/politics/2026/11/24/paris-hilton-joins-aoc-on-capitol-hill-to-fight-against-deepfake-porn/). A [corporate release reporting energy-sector financing](https://www.prnewswire.com/news-releases/peak-energy-expands-series-b-total-financing-to-157-million-to-accelerate-manufacturing-amid-growing-customer-backlog-302949087.html) covered a material business event and came from an interested party. These are examples a blanket topic, publisher, or sponsorship rule could demote unfairly.

The audit classified the [Pinkvilla film story](https://www.pinkvilla.com/entertainment/south/suriya50-suriya-to-work-with-dc-director-arun-matheswaran-for-blod-and-feast-action-film-heres-what-we-know-1406610) linked to `193` small-arms conflict in Japan as **metadata-only**. Its headline and description describe a film, but the extracted paragraphs were largely recommendations for other stories. The [Al Gore newspaper purchase](https://www.foxnews.com/media/al-gore-buys-hometown-tennessee-newspaper-vows-preserve-independent-voice) and [Tucson food giveaway](https://www.kvoa.com/news/good-news/vietnam-veteran-helps-provide-free-holiday-meals-at-tucson-food-city/article_1066bd4c-d92f-4ca2-9b64-a801e2adf48c.html) were body-verified and show the same Vietnam-background problem as the obituary. Of the 12 recent Vietnam cards, four had clear upstream mismatches on their displayed records, and three article bodies were unavailable.

## What the upstream fields can tell us

I compared **170 sampled event IDs across ten official Events and Mentions export pairs** with stored records. Every checked original CAMEO code, action country, and first-source URL agreed with GNM, and GNM retained all 170 codes. This checks transport and normalization for those fields and sample IDs. The publisher review tests whether the upstream values make sense. The same-slot Mentions check omits later mentions and separate translation files. [GDELT Events and Mentions codebook](https://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf) · [How GDELT joins Events, Mentions and GKG](https://blog.gdeltproject.org/complex-queries-combining-events-eventmentions-and-gkg/)

| Signal | Audit result and practical use |
| --- | --- |
| CAMEO code, actors, action geography, dates, URL | Most of these are already available. Show what was coded and flag contradictions for review. A country mention or actor affiliation can be background. Both obituary rows have an upstream date of November 25, so filtering out old event dates would miss them. |
| `ActionGeo_Type`, `Actor1CountryCode`, `Actor2CountryCode` | GNM’s event query asks for them, but the parser does not keep them. **Zero of 223,045 raw payloads** contained any of the three, despite 216,824 rows with coordinates. The obituary rows had country-level precision in the original file. Keeping these fields could distinguish centroids from more precise locations and clarify actor affiliations. The snapshot had zero stored coordinate pairs at `(0, 0)`. |
| `IsRootEvent` and `QuadClass` | GNM does not retain either. Both obituary rows were marked root events and verbal cooperation. A root-only rule would keep them and retain 5,692 of 10,673 records in the ten checked slots. QuadClass could support a labelled description of event composition as long as reporting and classification biases stay visible. |
| Mentions `SentenceID`, `InRawText`, `Confidence` | Both obituary rows and the traffic-crash protest row matched sentence 1, raw text, and confidence 100. The Vanuatu fisheries article and the New Zealand cyberattack article had sampled mentions at confidence 20. These fields can help rank alternative links for the same event. A hard threshold would discard verified relevant reporting. |
| `NumSources` and `NumArticles` | Only **316 of 10,673 records (3.0%)** in the checked slots had at least two sources. Both obituary records reported `NumArticles=10` and `NumSources=1`. Each had one matching source URL in the same-slot Mentions file. These legacy measures describe initial GDELT processing. Use distinct mention URLs in a bounded corroboration study and account for syndication and genuine single-source local reporting. |
| URL path and hostname | These fields support attribution and reversible sorting. The three sampled obituary-directory links were genuine obituaries, but the reported obituary and the trade-magazine obituary used other paths. The sports-path mask found only three distinct URLs across the two days and missed the MMA article. Sponsored content also varied in relevance. |

GNM’s `category_confidence` is a preset rule value, such as `0.90` for a Cyber keyword match. GDELT mention confidence is an upstream extraction signal. Neither verifies the article. GDELT’s Goldstein scale weights action codes, so a wrong action code can also produce a misleading conflict weight. [Parser](../src/connectors/gdelt/export_parser.py) · [Event detail query](../src/ingestion/dashboard_queries.py) · [Category rules](../src/domain/events/categorization.py)

## How the findings affect the numbers

The scenarios used the same two matched windows in read-only calculations. Omitting rows from **verified off-scope URLs** tests the observed sample only. The URL masks illustrate sensitivity to coarse rules. The resulting counts are hypothetical and every original record was preserved.

| Read-only scenario | Global change | Diplomacy change | What moved |
| --- | ---: | ---: | --- |
| Observed v1.2 | +5,411 (+4.97%) | +2,892 | 114,228 records in the current period |
| Omit rows referencing the 17 body-verified off-scope URLs | +5,398 (+4.96%) | +2,881 | 10 prior and 23 current records. The reported obituary itself fell after both matched windows. |
| Omit the specifically audited upstream mismatch IDs | +5,405 (+4.97%) | +2,889 | 10 prior and 16 current records. This scenario overlaps the one above. |
| Omit exact obituary directory names | +5,362 (+4.93%) | +2,838 | 31 distinct URLs and 137 records. The reported obituary used a different path. |
| Omit obituary, sports, entertainment and sponsored directory names | +5,350 (+4.95%) | +2,833 | 1,407 records, or 0.63% of both days. Syria Politics moved from +20.0% to +5.7% on a baseline of 35. |

The broad path experiment reduced the current world map by 697 geolocated records. The peak complete hour stayed at 00:00 UTC, falling from 8,066 to 8,008 records. Exact URL grouping also exposed concentration. **1,444 URLs with at least 20 records accounted for 42,636 records**, or **19.1%** of the two-day frame. The largest URL group contributed 173 records. A single article can support several genuine actions, and this concentration limits how much independent evidence a high record count implies.

The broad mask caught ten of the 17 body-verified off-scope URLs plus one borderline fundraising story. It missed all four off-scope URLs in the probability sample. The other flagged links came largely from the targeted-path checks. This is a small diagnostic, and the apparent success on deliberately selected paths would overstate how well the rule generalizes.

The GNM rule experiments moved category results more than the sample exclusions. Each probe disabled one rule at a time and kept every record. The unmodified categorizer reproduced the stored Cyber and Crisis results for both days.

| Category and probe | Observed prior → current | Counterfactual prior → current |
| --- | --- | --- |
| Cyber, remove the bare `breach` keyword | 376 → 529 (+153, +40.69%) | 236 → 360 (+124, +52.54%) |
| Cyber, require something beyond generic `064` Share intelligence or information | 376 → 529 (+153, +40.69%) | 335 → 465 (+130, +38.81%) |
| Crisis, require context beyond exact generic appeal codes `023`, `024`, `025` | 1,410 → 1,681 (+271, +19.22%) | 1,121 → 1,394 (+273, +24.35%) |

These changes measure sensitivity, with no assumption that every moved record is wrongly classified. Removing `breach` moved **169 of 529** current Cyber records across categories and changed the US comparison from **102 → 190** to **84 → 138**. The global Cyber percentage increased even as counts fell because the prior period lost a bigger share.

**Spikes and longer trends.** The matched-week anomaly check had zero complete comparable weeks for the world, US, and Vietnam and withheld its score. The seven-day comparison had 232 of 672 current slots and zero previous slots and withheld the change. The snapshot is too short to measure a real matched-week alert’s sensitivity. The legacy spike calculation could still rank results. Its baseline included UTC days with 28, 96, and 12 slots, and it omitted days with zero category/country counts. The top Bangladesh Protest row was 43 against an average of 8.5 and received a z-score of 69. Removing the verified off-scope URLs left the top-50 candidate set the same. The broad path mask changed one of the 50 candidates and one of the top ten. It also changed one of 50 actor pairs in the legacy Goldstein query. Sparse baseline coverage and ties limit what those rankings establish.

## Practical changes before a release

1. **Describe the cards as original GDELT links.** Put publisher, date, and event label in separate fields. Identify the event action and country as GDELT’s coding and make the event ID available for reporting a problem. Briefly explain that background mentions can appear alongside relevant coverage.
2. **Fix the parts GNM controls against a labelled holdout.** Test specific cyber context in place of bare `breach`, corroborate generic `064` with cyber context, and require humanitarian context for generic appeals. Review the scope of Diplomacy and the assignment of military posture to Protest. Capture geography precision and actor affiliations from the current feed. Keep the original category traceable and mark any definition change in trends. Reclassifying old summaries from 90-day aggregates alone is unreliable once event detail has expired.
3. **Try display-only prioritization first.** Keep every link and record available. In the offline ranking probe, at most two cards per hostname increased the worldwide page from eight to nine hosts and the Vietnam page from nine to 11. The US page already met the cap. This test measures diversity. The reported obituary can still surface. Test precise publisher genre metadata and code context alongside source diversity, with checks for local coverage and languages other than English.
4. **Pilot Mentions before adding another data feed.** On selected audited slots, rank alternative URLs for the same event by position and directness. Measure access and errors by language, country, source, and category, including relevant single-source stories. GKG themes or DOC search can be evaluated as topic context if needed. This work can run locally without paid services or ongoing publisher scraping.

The audit used the copied database in read-only transactions. It preserved the records and aggregates and changed no application filtering behavior. The local preview remained available, and Azure production was untouched.
