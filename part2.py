import datetime
from DbConnector import DbConnector
from tabulate import tabulate
from haversine import haversine, Unit

class ActivityTrackerProgram:
    def __init__(self):
        self.connection = DbConnector()
        self.db = self.connection.db

    def print_query_results(self, results, headers):
        print(tabulate(results, headers=headers, tablefmt='psql'))
        print()  # Add a blank line for readability

    # 1. Dataset counts
    def count_dataset_elements(self):
        user_count = self.db.users.count_documents({})
        activity_count = self.db.activities.count_documents({})
        trackpoint_count = self.db.activities.aggregate([
            {"$project": {"trackpoint_count": {"$size": "$trackpoints"}}},
            {"$group": {"_id": None, "total": {"$sum": "$trackpoint_count"}}}
        ]).next()['total']

        results = [[user_count, activity_count, trackpoint_count]]
        headers = ['Users', 'Activities', 'Trackpoints']
        print("1. Dataset counts:")
        self.print_query_results(results, headers)

    # 2. Average activities per user
    def average_activities_per_user(self):
        result = self.db.activities.aggregate([
            {"$group": {"_id": "$user_id", "activity_count": {"$sum": 1}}},
            {"$group": {"_id": None, "avg_activities": {"$avg": "$activity_count"}}}
        ]).next()
        
        results = [[round(result['avg_activities'], 2)]]
        headers = ['Average Activities per User']
        print("2. Average number of activities per user:")
        self.print_query_results(results, headers)

    # 3. Top 20 users with highest activity count
    def top_20_users_by_activity_count(self):
        results = list(self.db.activities.aggregate([
            {"$group": {"_id": "$user_id", "activity_count": {"$sum": 1}}},
            {"$sort": {"activity_count": -1}},
            {"$limit": 20},
            {"$project": {"_id": 0, "user_id": "$_id", "activity_count": 1}}
        ]))
        
        formatted_results = [[r['user_id'], r['activity_count']] for r in results]
        headers = ['User ID', 'Activity Count']
        print("3. Top 20 users with the highest number of activities:")
        self.print_query_results(formatted_results, headers)

    # 4. Users who have taken a taxi
    def users_who_took_taxi(self):
        results = list(self.db.activities.distinct(
            "user_id",
            {"transportation_mode": "taxi"}
        ))
        formatted_results = [[user_id] for user_id in sorted(results)]
        headers = ['User ID']
        print("4. Users who have taken a taxi:")
        self.print_query_results(formatted_results, headers)

    # 5. Count of activities for each transportation mode
    def count_transportation_modes(self):
        results = list(self.db.activities.aggregate([
            {"$match": {"transportation_mode": {"$ne": None}}},
            {"$group": {
                "_id": "$transportation_mode",
                "activity_count": {"$sum": 1}
            }},
            {"$sort": {"activity_count": -1}}
        ]))
        
        formatted_results = [[r['_id'], r['activity_count']] for r in results]
        headers = ['Transportation Mode', 'Activity Count']
        print("5. Count of activities for each transportation mode (excluding null):")
        self.print_query_results(formatted_results, headers)

    # 6. Year comparisons
    def compare_most_activities_and_hours(self):
        # Year with most activities
        activities_by_year = list(self.db.activities.aggregate([
            {"$group": {
                "_id": {"$year": "$start_date_time"},
                "activity_count": {"$sum": 1}
            }},
            {"$sort": {"activity_count": -1}},
            {"$limit": 1}
        ]))

        # Year with most recorded hours
        hours_by_year = list(self.db.activities.aggregate([
            {"$project": {
                "year": {"$year": "$start_date_time"},
                "duration_hours": {
                    "$divide": [
                        {"$subtract": ["$end_date_time", "$start_date_time"]},
                        3600000  # Convert milliseconds to hours
                    ]
                }
            }},
            {"$group": {
                "_id": "$year",
                "total_hours": {"$sum": "$duration_hours"}
            }},
            {"$sort": {"total_hours": -1}},
            {"$limit": 1}
        ]))

        # Print results for 6a
        activities_results = [[r['_id'], r['activity_count']] for r in activities_by_year]
        print("6a. Year with the most activities:")
        self.print_query_results(activities_results, ['Year', 'Activity Count'])

        # Print results for 6b
        hours_results = [[r['_id'], round(r['total_hours'], 2)] for r in hours_by_year]
        print("6b. Year with the most recorded hours:")
        self.print_query_results(hours_results, ['Year', 'Total Recorded Hours'])

        # Compare years
        activities_year = activities_by_year[0]['_id']
        hours_year = hours_by_year[0]['_id']
        
        if activities_year == hours_year:
            print(f"The year with the most activities ({activities_year}) "
                  f"is also the year with the most recorded hours.")
        else:
            print(f"The year with the most activities ({activities_year}) "
                  f"is different from the year with the most recorded hours ({hours_year}).")

    # 7. Total walking distance for user 112 in 2008
    def calculate_total_walking_distance_2008_user112(self):
        activities = list(self.db.activities.find({
            "user_id": "112",
            "transportation_mode": "walk",
            "start_date_time": {
                "$gte": datetime.datetime(2008, 1, 1),
                "$lt": datetime.datetime(2009, 1, 1)
            }
        }))

        total_distance = 0
        for activity in activities:
            trackpoints = activity['trackpoints']
            for i in range(len(trackpoints) - 1):
                point1 = (trackpoints[i]['lat'], trackpoints[i]['lon'])
                point2 = (trackpoints[i + 1]['lat'], trackpoints[i + 1]['lon'])
                distance = haversine(point1, point2, unit=Unit.KILOMETERS)
                total_distance += distance

        print("\n7. Total distance walked in 2008 by user with id=112:")
        print(f"   {total_distance:.2f} km")

    # 8. Top 20 users by altitude gain
    def top_20_users_by_altitude_gain(self):
        results = list(self.db.activities.aggregate([
            {"$unwind": "$trackpoints"},
            {"$project": {
                "user_id": 1,
                "trackpoints": 1,
                "prev_altitude": {
                    "$arrayElemAt": [
                        "$trackpoints.altitude",
                        {"$subtract": [{"$indexOfArray": ["$trackpoints", "$trackpoints"]}, 1]}
                    ]
                }
            }},
            {"$match": {
                "trackpoints.altitude": {"$ne": -777},
                "prev_altitude": {"$ne": -777}
            }},
            {"$project": {
                "user_id": 1,
                "altitude_gain": {
                    "$max": [
                        {"$subtract": ["$trackpoints.altitude", "$prev_altitude"]},
                        0
                    ]
                }
            }},
            {"$group": {
                "_id": "$user_id",
                "total_altitude_gain": {"$sum": "$altitude_gain"}
            }},
            {"$sort": {"total_altitude_gain": -1}},
            {"$limit": 20}
        ]))

        # Convert feet to meters
        formatted_results = [
            [r['_id'], round(r['total_altitude_gain'] * 0.3048, 2)]
            for r in results
        ]
        
        headers = ['User ID', 'Total Meters Gained']
        print("\n8. Top 20 users who have gained the most altitude meters:")
        self.print_query_results(formatted_results, headers)

    # 9. Users with invalid activities
    def find_users_with_invalid_activities(self):
        invalid_activities = []
        
        activities = self.db.activities.find({})
        for activity in activities:
            trackpoints = activity['trackpoints']
            is_invalid = False
            
            for i in range(len(trackpoints) - 1):
                time1 = trackpoints[i]['date_time']
                time2 = trackpoints[i + 1]['date_time']
                time_diff = (time2 - time1).total_seconds() / 60
                
                if time_diff >= 5:
                    is_invalid = True
                    break
            
            if is_invalid:
                invalid_activities.append(activity['user_id'])

        # Count invalid activities per user
        user_counts = {}
        for user_id in invalid_activities:
            user_counts[user_id] = user_counts.get(user_id, 0) + 1

        formatted_results = [[user_id, count] for user_id, count in 
                           sorted(user_counts.items(), key=lambda x: x[1], reverse=True)]
        
        headers = ['User ID', 'Invalid Activity Count']
        print("\n9. Users with invalid activities and their count:")
        self.print_query_results(formatted_results, headers)

    # 10. Users who have tracked activity in the Forbidden City
    def find_users_in_forbidden_city(self):
        results = list(self.db.activities.aggregate([
            {"$unwind": "$trackpoints"},
            {"$match": {
                "$expr": {
                    "$and": [
                        {"$eq": [{"$round": ["$trackpoints.lat", 3]}, 39.916]},
                        {"$eq": [{"$round": ["$trackpoints.lon", 3]}, 116.397]}
                    ]
                }
            }},
            {"$group": {"_id": "$user_id"}},
            {"$sort": {"_id": 1}}
        ]))

        formatted_results = [[r['_id']] for r in results]
        headers = ['User ID']
        print("\n10. Users who have tracked an activity in the Forbidden City of Beijing:")
        self.print_query_results(formatted_results, headers)

    # 11. Users' most used transportation mode
    def find_users_most_used_transportation(self):
        results = list(self.db.activities.aggregate([
            {"$match": {"transportation_mode": {"$ne": None}}},
            {"$group": {
                "_id": {
                    "user_id": "$user_id",
                    "mode": "$transportation_mode"
                },
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}},
            {"$group": {
                "_id": "$_id.user_id",
                "most_used_mode": {"$first": "$_id.mode"}
            }},
            {"$sort": {"_id": 1}}
        ]))

        formatted_results = [[r['_id'], r['most_used_mode']] for r in results]
        headers = ['User ID', 'Most Used Transportation Mode']
        print("\n11. Users with registered transportation_mode and their most used mode:")
        self.print_query_results(formatted_results, headers)

def main():
    program = None
    try:
        program = ActivityTrackerProgram()
        
        # Execute all queries
        program.count_dataset_elements()
        program.average_activities_per_user()
        program.top_20_users_by_activity_count()
        program.users_who_took_taxi()
        program.count_transportation_modes()
        program.compare_most_activities_and_hours()
        program.calculate_total_walking_distance_2008_user112()
        program.top_20_users_by_altitude_gain()
        program.find_users_with_invalid_activities()
        program.find_users_in_forbidden_city()
        program.find_users_most_used_transportation()
        
    except Exception as e:
        print("An error occurred:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()