#%%
## Generate a full season's worth of batting Marcel projections from past years' stats

# 1. Daily and Full Season should use slightly different Marcel's starting points right?
# 2. Can anything be easily automated
# 4. Comment through everything
# 6. Add Fanduel and Underdog Points
# 7. Clean up this folder and commit anything outstanding

from createTuple import createTuple ## gist: 778481
from writeMatrixCSV import writeMatrixCSV ## gist: 778484

def makeBatTable(r):
    r['AB'] = r['PA'] - r['BB'] - r['IBB'] - r['SF'] - r['HP'] - r['SH']
    for stat in ['AB', 'H', 'S', 'D', 'T', 'HR', 'SO', 'BB', 'SF', 'HP']:
        if stat in r:   pass
        else:   r[stat] = 0
    if r['AB'] == 0:
        r['SLG'] = 0
        r['AVG'] = 0
        slg = 0
    else:
        avg = float(r['H'])/float(r['AB'])
        r['AVG'] = round(avg, 3)
        slg = float(r['H']+r['D']+(2*r['T'])+(3*r['HR']))/float(r['AB'])
        r['SLG'] = round(slg, 3)
    if (r['AB']+r['BB']+r['SF']+r['HP']) == 0:
        r['OBP'] = 0
        r['OPS'] = 0
        r['wOBA'] = 0
        r['DKp'] = 0
        r['DKp/PA'] = 0
    else:
        pa = float(r['PA'])
        obp = float(r['H']+r['BB']+r['HP'])/pa
        r['OBP'] = round(obp, 3)
        sing = int(r['H']) - int(r['HR']) - int(r['T']) - int(r['D'])
        num = (.72*int(r['BB'])) + (.75*int(r['HP'])) + (.9*sing) + (1.24*int(r['D'])) + (1.56*int(r['T'])) + (1.95*int(r['HR']))
        den = int(r['BB']) + int(r['AB']) + int(r['HP'])
        woba = num/den
        r['wOBA'] = round(woba, 3)
        DK = (3*sing) + (5*int(r['D'])) + (8*int(r['T'])) + (10*int(r['HR'])) + (2*int(r['RBI'])) + (2*int(r['R'])) + (2*int(r['BB'])) + (2*int(r['HP'])) + (5*int(r['SB']))
        r['DKp'] = round(DK, 0)
        r['DKp/PA'] = round(DK / pa, 3)
    return r
    
def marcelBattingSeason(yr):
    # yr = year being projected, input as int
    yr = str(yr) 
    yr1 = str(int(yr) - 1)
    yr2 = str(int(yr) - 2)
    yr3 = str(int(yr) - 3)

    ## get list of pitchers; determine which batters are really
    ## 'batters' and throw out pitchers with at-bats
    ## projectBatters will be a list of any true batters who has accumulated 
    ## counting stats over any of the previous 3 seasons
    projectBatters = []
    for yr in [yr3, yr2, yr1, yr]:
        yearPitchers = {}
        for p in pitchers:
            pitchID = p[25]
            if p[0] == yr:
                yearPitchers[pitchID] = int(p[14])
            else:   pass
        for b in batters:
            batID = b[23]
            if b[0] == yr:  pass
            else:   continue
            abString = b[5]
            if abString == '' or abString == '0':  continue
            else:   batAb = int(abString)
            if batID in yearPitchers:
                if yearPitchers[batID] > batAb and batID != '19755': continue
                else:   pass
            else:   pass
            if batID in projectBatters: pass
            else:   projectBatters.append(batID)

    for p in roles:
        playerID = p[0]
        role = p[2]
        if role in batting_order_season_pa:
            if playerID in projectBatters: pass
            else: projectBatters.append(playerID)
        else: pass

    ## find league average for previous year
    yearPitchers = {}
    for p in pitchers:
        pitchID = p[25]
        if p[0] == yr1:
            yearPitchers[pitchID] = int(p[14])
        else:   pass

    leagueAverage = {}
    for b in batters:
        batID = b[23]
        if b[0] == yr1:  pass
        else:   continue
        abString = b[5]
        if abString == '' or abString == '0':  continue
        else:   batAb = int(abString)
        if batID in yearPitchers:
            if yearPitchers[batID] > batAb and batID != '19755': continue
            else:   pass
        else:   pass
        if batID in projectBatters: pass
        else:   projectBatters.append(batID)
        for stat in batHeaders:
            col = batHeaders[stat]
            try:    playerStat = int(float(b[col]))
            except: continue
            else:   pass
            if stat in leagueAverage:
                leagueAverage[stat] += playerStat
            else:
                leagueAverage[stat] = playerStat

    for stat in ['HP', 'SF', 'SH']:
        if stat in leagueAverage:   pass
        else:   leagueAverage[stat] = 0
    totalPa = leagueAverage['PA']
    regression = {}
    for stat in leagueAverage:
        regression[stat] =(1200.0/totalPa)*leagueAverage[stat]

    rawProjections = {}
    ## calculate projections for each player
    for b in projectBatters:
        components = {}
        y2pa = 0
        y1pa = 0
        for stat in batHeaders:
            components[stat] = 0
        for row in batters:
            if row[23] == b: pass
            else:   continue
            if row[0] == yr3:
                for stat in batHeaders:
                    try:    playerStat = int(float(row[batHeaders[stat]]))
                    except: continue
                    components[stat] += 3*playerStat
            elif row[0] == yr2:
                for stat in batHeaders:
                    try:    playerStat = int(float(row[batHeaders[stat]]))
                    except: continue
                    components[stat] += 4*playerStat
                
                for stat in ['PA']:
                    try:    y2pa += int(float(row[batHeaders[stat]]))
                    except: continue                
            elif row[0] == yr1:
                for stat in batHeaders:
                    try:    playerStat = int(float(row[batHeaders[stat]]))
                    except: continue
                    components[stat] += 5*playerStat
                
                for stat in ['PA']:
                    try:    y1pa += int(float(row[batHeaders[stat]]))
                    except: continue
            else:   continue
        ## add regression component
        for stat in regression:
            if stat in components:
                components[stat] += regression[stat]
            else:
                components[stat] = regression[stat]
        ## get projected PA
        player_hist_projPa = 0.5*y1pa + 0.1*y2pa + 200
        rolePa = player_hist_projPa
        player_park = 0
        for row in roles:
            if row[0] == b: pass
            else: continue
            player_park = row[5]
            if row[2] in batting_order_season_pa:
                rolePa = batting_order_season_pa[row[2]]
            else:
                if row[0] == '19755': continue
                rolePa = 0
        # Can def play around with this
        projPa = (player_hist_projPa * 0.5) + (rolePa * 0.5)
        ## prorate into projected PA
        compPa = components['PA']
        prorateProj = {}
        for stat in components:
            prorateProj[stat] = (projPa/compPa)*components[stat]
        prorateProj['PA'] = projPa
        parkProj = {}
        for stat in prorateProj:
            if stat in ['S', 'D', 'T', 'HR', 'SO', 'BB']:
                for row in parks:
                    if row[0] == player_park:
                        parkProj[stat] = prorateProj[stat] * (((int(row[park_stat_columns[stat]]) + 100) / 2) / 100)
                    else: parkProj[stat] = prorateProj[stat]
            else:
                parkProj[stat] = prorateProj[stat]
        try:    age = int(yr) - int(float(birthYear[b]))
        except: age = 29 ## in case birthyear is missing or corrupted
        ## age adjust
        if age > 29:
            ageAdj = 1/(1 + ((age - 29)*0.003))
        elif age < 29:
            ageAdj = 1 + ((29 - age)*0.006)
        else:
            ageAdj = 1
        finalProj = {}
        for stat in parkProj:
            if stat in ['PA', 'AB']:
                finalProj[stat] = parkProj[stat]
            elif stat in ['R', 'H', 'S', 'D', 'T', 'HR', 'RBI', 'SB', 'BB', 'IBB', 'HP', 'SH', 'SF']:
                finalProj[stat] = parkProj[stat]*ageAdj
            else:
                finalProj[stat] = parkProj[stat]/ageAdj
        ## reliability
        reliab = 1 - (1200.0/compPa)
        finalProj['rel'] = round(reliab, 2)
        finalProj['Age'] = age
        ## add to master dict
        rawProjections[b] = finalProj

    ## re-baseline
    projTotal = {}
    for pl in rawProjections:
        for stat in rawProjections[pl]:
            if stat in projTotal:
                projTotal[stat] += rawProjections[pl][stat]
            else:
                projTotal[stat] = rawProjections[pl][stat]
    projTotalPa = projTotal['PA']
    projRatios = {}
    for stat in ['PA', 'AB', 'R', 'S', 'H', 'D', 'T', 'HR', 'RBI', 'SB', 'CS', 'BB', 'SO', 'IBB', 'HP', 'SH', 'SF', 'GDP']:
        projRatios[stat] = projTotal[stat]/projTotalPa

    trueRatios = {}
    for stat in ['PA', 'AB', 'R', 'S', 'H', 'D', 'T', 'HR', 'RBI', 'SB', 'CS', 'BB', 'SO', 'IBB', 'HP', 'SH', 'SF', 'GDP']:
        try:    trueRatios[stat] = leagueAverage[stat]/float(totalPa)
        except: trueRatios[stat] = 0

    marcels = {}
    for pl in rawProjections:
        marcels[pl] = {}
        for stat in rawProjections[pl]:
            if stat in ['AB', 'R', 'S', 'H', 'D', 'T', 'HR', 'RBI', 'SB', 'CS', 'BB', 'SO', 'IBB', 'HP', 'SH', 'SF', 'GDP']:
                if projRatios[stat] == 0:
                    marcels[pl][stat] = rawProjections[pl][stat]
                else:
                    marcels[pl][stat] = round((trueRatios[stat]/projRatios[stat])*rawProjections[pl][stat], 0)
            elif stat == 'PA':
                marcels[pl][stat] = round(rawProjections[pl][stat], 0)
            else:
                marcels[pl][stat] = rawProjections[pl][stat]

    header = ['playerid', 'Name', 'Year', 'Team', 'age', 'rel', 'DraftKingsPoints', 'DKp/PA', 'wOBA', 'AVG', 'OBP', 'SLG',
                     'PA', 'AB', 'R', 'H', 'S', 'D', 'T', 'HR', 'RBI',
                     'SB', 'CS', 'BB', 'SO', 'IBB', 'HP', 'SH', 'SF', 'GDP']

    marcelSheet = [header]
    for pl in marcels:
        row = [pl]
        try: row += firstlast[pl]
        except: 
            for r in roles:
                if r[0] == pl: row += [r[1]]
                else: continue
            if len(row) < 2: continue
        row.append(yr)
        for r in roles:
            if r[0] == pl and len(row) < 4: row += [r[3]]
            else: continue
        if len(row) < 4: row.append('FA')
        else: pass
        marcels[pl] = makeBatTable(marcels[pl])
        for stat in ['Age', 'rel', 'DKp', 'DKp/PA', 'wOBA', 'AVG', 'OBP', 'SLG',
                     'PA', 'AB', 'R', 'H', 'S', 'D', 'T', 'HR', 'RBI',
                     'SB', 'CS', 'BB', 'SO', 'IBB', 'HP', 'SH', 'SF', 'GDP']:
            row.append(marcels[pl][stat])
        marcelSheet.append(row)
    filename = 'full_season_batters_' + yr + '.csv'
    writeMatrixCSV(marcelSheet, filename)

batHeaders = {'AB': 4,
              'PA': 5,
              'R': 11,
              'H': 6,
              'S': 7,
              'D': 8,
              'T': 9,
              'HR': 10,
              'RBI': 12,
              'SB': 20,
              'CS': 21,
              'BB': 13,
              'SO': 15,
              'IBB': 14,
              'HP': 16,
              'SH': 18,
              'SF': 17,
              'GDP': 19
              }

park_stat_columns = {
    'S': 3,
    'D': 4,
    'T': 5,
    'HR': 6,
    'SO': 7,
    'BB': 8
}

# Could lower Bench threshold if necessary
batting_order_season_pa = {
    '1': 743.6,
    '2': 725.6,
    '3': 707.3,
    '4': 690.7,
    '5': 674.1,
    '6': 657.6,
    '7': 639.5,
    '8': 620.9,
    '9': 601.2,
    'B': 250
}


pitchers = createTuple('pitching_data_17-21.csv')
## this is the pitcher seasons sheet from the lahman db.  headers:
## playerID,yearID,stint,teamID,lgID,W,L,G,GS,CG,SHO,SV,Ipouts,H,ER,HR,BB,SO,Baopp,ERA,IBB,WP,HP,BK,BFP,GF,R

batters = createTuple('batting_data_17-21.csv')
## this is the batting seasons sheet from the lahman db.  headers:
## playerID,yearID,stint,teamID,lgID,G,G_batting,AB,R,H,D,T,HR,RBI,SB,CS,BB,SO,IBB,HP,SH,SF,GIDP,G_old

## master db for birthYear
master = createTuple('player_id_table.csv')
## master biographical data sheet from lahman db.  headers:
## lahmanID,playerID,managerID,hofID,birthYear,birthMonth,birthDay,birthCountry,birthState,birthCity,deathYear,deathMonth,deathDay,deathCountry,deathState,deathCity,nameFirst,nameLast,nameNote,nameGiven,nameNick,weight,height,bats,throws,debut,finalGame,college,lahman40ID,lahman

roles = createTuple('test_selenium_df1.csv')
parks = createTuple('ParkFactors.csv')

birthYear = {}
for pl in master:
    if pl[2] == 'birthdate' or '': continue
    dob = pl[2]
    year = int(dob.split('/')[2]) - 100 + 2000
    if year < 1950: year += 100
    birthYear[pl[8]] = year

# Can switch this back to player id table
firstlast = {}
for pl in master:
    full_name = pl[9]
    firstlast[pl[8]] = [full_name]

## Call this function to create projections
marcelBattingSeason(2022)
# %%
