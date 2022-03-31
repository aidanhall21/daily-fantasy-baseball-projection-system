#%%
## Generate a full season's worth of pitching Marcel projections from past years' stats
## K% and BB% and WHIP

from createTuple import createTuple ## gist: 778481
from writeMatrixCSV import writeMatrixCSV ## gist: 778484

def makePitTable(r):
    r['TBF'] = (r['IP'] * 3) + r['H'] + r['BB'] + r['IBB'] + r['HP']
    for stat in ['H', 'SO', 'BB','HP', 'R']:
        if stat in r:   pass
        else:   r[stat] = 0
    svOpp = r['SV'] + r['BS']
    if svOpp == 0:
        r['SV%'] = 0
    else:
        r['SV%'] = round(float(r['SV']) / (svOpp), 2)
    decisions = r['W'] + r['L']
    if decisions == 0:
        r['W%'] = 0
    else:
        r['W%'] = round(float(r['W']) / decisions, 2)
    ip = float(r['IP'])
    era = float(r['ER']*9)/ip
    ra = float(r['R']*9)/ip
    r['ERA'] = round(era, 2)
    r['RA'] = round(ra, 2)
    r['IP'] = round(ip, 1)
    DK = (2.25*ip) + (2*int(r['SO'])) + (4*int(r['W'])) - (2*int(r['ER'])) - (0.6*int(r['H'])) - (0.6*int(r['BB'])) - (0.6*int(r['HP'])) + (2.5*int(r['CG'])) + (2.5*int(r['SHO']))
    r['DKp'] = round(DK, 0)
    r['DKp/BF'] = round(DK/r['TBF']+0.001, 3)
    
    return r

def marcelPitchingSeason(yr):
    # yr = year being projected, input as int
    yr = str(yr) 
    yr1 = str(int(yr) - 1)
    yr2 = str(int(yr) - 2)
    yr3 = str(int(yr) - 3)

    ## get list of batters; determine which pitchers are really
    ## 'pitchers' and throw out hitters with pitching lines
    projectPitchers = []
    for yr in [yr3, yr2, yr1, yr]:
        yearBatters = {}
        for b in batters:
            batID = b[23]
            if b[0] == yr:
                if b[5] == '' or b[5] == '0':  continue
                else:   pass
                yearBatters[batID] = int(b[5])
            else:   pass
        for p in pitchers:
            pitID = p[25]
            if p[0] == yr:  pass
            else:   continue
            ipString = p[14]
            if ipString == '' or ipString == '0':  continue
            else:   pitIp = int(float(ipString))
            if pitID in yearBatters:
                if yearBatters[pitID] > pitIp and pitID != '19755': continue
                else:   pass
            else:   pass
            if pitID in projectPitchers: pass
            else:   projectPitchers.append(pitID)

    for p in roles:
        playerID = p[0]
        role = p[2]
        if role in pitcher_roles:
            if playerID in projectPitchers: pass
            else: projectPitchers.append(playerID)
    else: pass

    ## find league average for previous year
    yearBatters = {}
    for b in batters:
        batID = b[23]
        if b[0] == yr1:
            if b[5] == '' or b[5] == '0':  continue
            else:   pass
            yearBatters[batID] = int(b[5])
        else:   pass

    leagueAverage = {}
    for p in pitchers:
        pitID = p[25]
        if p[0] == yr1:  pass
        else:   continue
        ipString = p[14]
        if ipString == '' or ipString == '0':  continue
        else:   pitIp = int(float(ipString))
        if pitID in yearBatters:
            if yearBatters[pitID] > pitIp and pitID != '19755': continue
            else:   pass
        else:   pass
        if pitID in projectPitchers: pass
        else:   projectPitchers.append(pitID)
        for stat in pitHeaders:
            col = pitHeaders[stat]
            try:    playerStat = int(float(p[col]))
            except: continue
            else:   pass
            if stat in leagueAverage:
                leagueAverage[stat] += playerStat
            else:
                leagueAverage[stat] = playerStat

    for stat in pitHeaders:
        if stat in leagueAverage:   pass
        else:   leagueAverage[stat] = 0
    totalPa = leagueAverage['TBF']
    league_regression = {}
    for stat in leagueAverage:
        league_regression[stat] =(1200.0/totalPa)*leagueAverage[stat]

    rawProjections = {}
    ## generate projections for list of pitchers
    for p in projectPitchers:
        regression = dict(league_regression)
        components = {}
        y2ip = 0
        y1ip = 0
        for stat in pitHeaders:
            components[stat] = 0
        for row in pitchers:
            if row[25] == p: pass
            else:   continue
            if row[0] == yr3:
                for stat in pitHeaders:
                    try:    playerStat = int(float(row[pitHeaders[stat]]))
                    except: continue
                    components[stat] += playerStat
            elif row[0] == yr2:
                for stat in pitHeaders:
                    try:    playerStat = int(float(row[pitHeaders[stat]]))
                    except: continue
                    components[stat] += 2*playerStat
                y2ip += int(float(row[pitHeaders['IP']]))
            elif row[0] == yr1:
                for stat in pitHeaders:
                    try:    playerStat = int(float(row[pitHeaders[stat]]))
                    except: continue
                    components[stat] += 3*playerStat
                y1ip += int(float(row[pitHeaders['IP']]))
            else:   continue
        try:
            ggs = components['GS']/float(components['G'])
            ipReg = 25 + ggs*35
        except:
            ipReg = 25
        ## add regression component
        hasRole = 0
        for row in roles:
            if row[0] == p:
                hasRole = 1
                pass
            else: continue
            if row[2].startswith('SP'):
                    regression['G'] = regression['GS']
                    for stat in ['SV', 'HLD', 'BS']:
                        regression[stat] = 0
            else:
                regression['GS'] = 0

        if hasRole == 1:
            pass
        else:
            regression['GS'] = (regression['GS'] * ggs)
            regression['G'] = regression['GS'] + (regression['G'] * (1 - ggs))
            for stat in ['SV', 'HLD', 'BS']:
                regression[stat] = regression[stat] * (1 - ggs)
        
        for stat in regression:
            components[stat] += regression[stat]

        ## get projected PA
        pitcher_hist_projIp = 0.5*y1ip + 0.1*y2ip + ipReg
        roleIp = pitcher_hist_projIp
        player_park = 0
        for row in roles:
            if row[0] == p: pass
            else: continue
            player_park = row[5]
            if row[2] in pitcher_roles:
                roleIp = pitcher_roles[row[2]]
            else:
                if row[0] == '19755': continue
                roleIp = 0
        ## prorate into projected PA
        projIp = (pitcher_hist_projIp * 0.5) + (roleIp * 0.5)
        compIp = components['IP']
        prorateProj = {}
        for stat in components:
            prorateProj[stat] = (projIp/compIp)*components[stat]
        prorateProj['IP'] = projIp
        parkProj = {}
        for stat in prorateProj:
            if stat in ['H', 'HR', 'SO', 'BB']:
                for row in parks:
                    if row[0] == player_park:
                        parkProj[stat] = prorateProj[stat] * (((int(row[park_stat_columns[stat]]) + 100) / 2) / 100)
                    else: parkProj[stat] = prorateProj[stat]
            else:
                parkProj[stat] = prorateProj[stat]
        ## get age
        try:    age = int(yr) - int(float(birthYear[p]))
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
            if stat in ['G', 'GS', 'IP', 'TBF']:
                finalProj[stat] = parkProj[stat]
            elif stat in ['W', 'CG', 'SHO', 'SV', 'SO', 'HLD']:
                finalProj[stat] = parkProj[stat]*ageAdj
            else:
                finalProj[stat] = parkProj[stat]/ageAdj
        ## reliability
        reliab = 1 - (1200.0/components['TBF'])
        finalProj['rel'] = round(reliab, 2)
        finalProj['Age'] = age
        ## add to master dict
        rawProjections[p] = finalProj

    ## re-baseline
    projTotal = {}
    for pl in rawProjections:
        for stat in rawProjections[pl]:
            if stat in projTotal:
                projTotal[stat] += rawProjections[pl][stat]
            else:
                projTotal[stat] = rawProjections[pl][stat]
    projTotalBf = projTotal['TBF']
    projRatios = {}
    for stat in ['W', 'L', 'G', 'GS', 'CG', 'SHO', 'SV', 'HLD', 'BS', 'IP', 'TBF', 'H', 'ER', 'HR', 'BB', 'SO',
                 'IBB', 'WP', 'HP', 'BK', 'R']:
        projRatios[stat] = projTotal[stat]/projTotalBf

    trueRatios = {}
    for stat in ['W', 'L', 'G', 'GS', 'CG', 'SHO', 'SV', 'HLD', 'BS', 'IP', 'TBF', 'H', 'ER', 'HR', 'BB', 'SO',
                 'IBB', 'WP', 'HP', 'BK', 'R']:
        try:    trueRatios[stat] = leagueAverage[stat]/float(totalPa)
        except: trueRatios[stat] = 0

    marcels = {}
    for pl in rawProjections:
        marcels[pl] = {}
        for stat in rawProjections[pl]:
            if stat in ['TBF', 'H', 'ER', 'HR', 'BB', 'SO',
                 'IBB', 'WP', 'HP', 'BK', 'R']:
                if projRatios[stat] == 0:
                    marcels[pl][stat] = rawProjections[pl][stat]
                else:
                    marcels[pl][stat] = round((trueRatios[stat]/projRatios[stat])*rawProjections[pl][stat], 0)
            elif stat in ['IP', 'W', 'L', 'G', 'GS', 'CG', 'SHO', 'SV', 'HLD', 'BS']:
                marcels[pl][stat] = round(rawProjections[pl][stat], 0)
            else:
                marcels[pl][stat] = rawProjections[pl][stat]
##
    header = ['playerid', 'Name', 'Year', 'Team', 'Age', 'rel', 'DraftKingsPoints', 'DKp/BF',
              'ERA', 'RA', 'W', 'L', 'W%', 'G', 'GS', 'CG', 'SHO', 'SV%', 'SV', 'HLD', 'BS', 'IP', 'TBF', 'H', 'ER', 'HR', 'BB', 'SO',
                 'IBB', 'WP', 'HP', 'BK', 'R']

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
        marcels[pl] = makePitTable(marcels[pl])
        for stat in ['Age', 'rel', 'DKp', 'DKp/BF',
              'ERA', 'RA', 'W', 'L', 'W%', 'G', 'GS', 'CG', 'SHO', 'SV%', 'SV', 'HLD', 'BS', 'IP', 'TBF', 'H', 'ER', 'HR', 'BB', 'SO',
                 'IBB', 'WP', 'HP', 'BK', 'R']:
            row.append(marcels[pl][stat])
        marcelSheet.append(row)

    filename = 'full_season_pitchers_' + yr + '.csv'
    writeMatrixCSV(marcelSheet, filename)

pitHeaders = {'W': 3,
              'L': 4,
              'G': 6,
              'GS': 7,
              'CG': 8,
              'SHO': 9,
              'SV': 10,
              'HLD': 11,
              'BS': 12,
              'IP': 13,
              'TBF': 14,
              'H': 15,
              'ER': 17,
              'HR': 18,
              'BB': 19,
              'SO': 24,
              'IBB': 20,
              'WP': 22,
              'HP': 21,
              'BK': 23,
              'R': 16
              }

park_stat_columns = {
    'H': 2,
    'HR': 6,
    'SO': 7,
    'BB': 8
}

pitcher_roles = {
    'CL': 75,
    'LR': 75,
    'MID': 75,
    'SP1': 200,
    'SP2': 185,
    'SP3': 160,
    'SP4': 140,
    'SP5': 120,
    'SU7': 75,
    'SU8': 75
}


pitchers = createTuple('pitching_data_17-21.csv')


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

## sample usage 
marcelPitchingSeason(2022)
# %%
