from pymongo import MongoClient, ASCENDING
import datetime
import os
from typing import List, Dict, Any
from tabulate import tabulate
from DbConnector import DbConnector

class ActivityTrackerProgram:
    def __init__(self):
        self.connection = DbConnector()
        self.db = self.connection.db

    def create_collections(self):
        # Create indexes
        self.db.users.create_index([("user_id", ASCENDING)], unique=True)
        self.db.activities.create_index([("activity_id", ASCENDING)], unique=True)
        self.db.activities.create_index([("user_id", ASCENDING)])
        print("Collections and indexes created successfully")

    def drop_collections(self):
        self.db.users.drop()
        self.db.activities.drop()
        print("Collections dropped successfully")

    def insert_user_data(self, user_id: str, has_labels: bool):
        user_doc = {
            "user_id": user_id,
            "has_labels": has_labels
        }
        try:
            self.db.users.insert_one(user_doc)
        except Exception as e:
            print(f"Error inserting user {user_id}: {e}")

    def insert_activity_data(self, activity_id: int, user_id: str, activity_data: Dict):
        activity_doc = {
            "activity_id": activity_id,
            "user_id": user_id,
            "transportation_mode": None,
            "start_date_time": activity_data['start_date_time'],
            "end_date_time": activity_data['end_date_time'],
            "trackpoints": []  # Will be populated with trackpoints
        }
        try:
            self.db.activities.insert_one(activity_doc)
        except Exception as e:
            print(f"Error inserting activity {activity_id}: {e}")

    def insert_trackpoints_batch(self, activity_id: int, trackpoints: List[tuple]):
        trackpoint_docs = []
        for tp in trackpoints:
            trackpoint_doc = {
                "lat": tp[1],
                "lon": tp[2],
                "altitude": tp[3],
                "date_days": tp[4],
                "date_time": tp[5]
            }
            trackpoint_docs.append(trackpoint_doc)

        try:
            self.db.activities.update_one(
                {"activity_id": activity_id},
                {"$push": {"trackpoints": {"$each": trackpoint_docs}}}
            )
        except Exception as e:
            print(f"Error inserting trackpoints for activity {activity_id}: {e}")

    def fetch_data(self, collection_name: str):
        docs = list(self.db[collection_name].find({}, {'_id': 0}))
        if docs:
            headers = docs[0].keys()
            rows = [[str(doc[header]) for header in headers] for doc in docs]
            print(f"Data from collection {collection_name}, tabulated:")
            print(tabulate(rows, headers=headers))
        return docs

    def show_collections(self):
        collections = self.db.list_collection_names()
        print("Collections in database:")
        for collection in collections:
            print(f"- {collection}")

    def populate_user_table(self, dataset_path: str):
        labeled_ids_path = os.path.join(dataset_path, 'dataset', 'labeled_ids.txt')
        
        with open(labeled_ids_path, 'r') as f:
            labeled_ids = set(f.read().splitlines())
        
        processed_users = set()
        data_path = os.path.join(dataset_path, 'dataset', 'Data')
        
        for root, dirs, files in os.walk(data_path):
            for dir in dirs:
                if dir == "Trajectory":
                    continue
                    
                user_id = dir
                if user_id in processed_users:
                    continue

                has_labels = user_id in labeled_ids
                self.insert_user_data(user_id, has_labels)
                processed_users.add(user_id)
        
        print("Users collection populated successfully")

    def populate_activities(self, dataset_path: str):
        data_path = os.path.join(dataset_path, 'dataset', 'Data')
        
        for root, dirs, files in os.walk(data_path):
            if 'Trajectory' in root:
                user_id = os.path.basename(os.path.dirname(root))
                for file in files:
                    if file.endswith('.plt'):
                        activity_id_str = f"{user_id}{os.path.splitext(file)[0]}"
                        try:
                            activity_id = int(activity_id_str)
                        except ValueError:
                            print(f"Invalid activity_id generated: {activity_id_str}")
                            continue

                        file_path = os.path.join(root, file)
                        activity_data = self.process_activity_file(file_path)
                        
                        if activity_data:
                            self.insert_activity_data(activity_id, user_id, activity_data)
                            trackpoints = self.process_trackpoints(file_path, activity_id)
                            if trackpoints:
                                self.insert_trackpoints_batch(activity_id, trackpoints)
        
        print("Activities collection populated successfully")

    def process_activity_file(self, file_path: str) -> Dict:
        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()[6:]
                
                if len(lines) > 2500:
                    print(f"Skipping file {file_path} due to too many trackpoints ({len(lines)}).")
                    return None

                start_time = None
                end_time = None
                
                for line_num, line in enumerate(lines, start=7):
                    try:
                        parts = line.strip().split(',')
                        if len(parts) >= 7:
                            date = parts[5]
                            time = parts[6]
                            date_string = f"{date} {time}"
                            current_time = datetime.datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
                            
                            if start_time is None:
                                start_time = current_time
                            end_time = current_time
                    except Exception as e:
                        print(f"Error processing line {line_num} in file {file_path}: {e}")
                        continue

                if start_time and end_time:
                    return {
                        'start_date_time': start_time,
                        'end_date_time': end_time
                    }
                return None
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            return None

    def process_trackpoints(self, file_path: str, activity_id: int) -> List[tuple]:
        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()[6:]
                
                if len(lines) > 2500:
                    return None

                trackpoints = []
                for line in lines:
                    parts = line.strip().split(',')
                    if len(parts) >= 7:
                        lat, lon = float(parts[0]), float(parts[1])
                        altitude = int(float(parts[3]))
                        date_days = float(parts[4])
                        date_string = f"{parts[5]} {parts[6]}"
                        date_time = datetime.datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
                        
                        trackpoints.append((activity_id, lat, lon, altitude, date_days, date_time))

                return trackpoints
        except Exception as e:
            print(f"Error processing trackpoints in file {file_path}: {e}")
            return None

    def update_transportation_modes(self, dataset_path: str):
        labels = self.read_labels(dataset_path)
        users_with_labels = self.get_users_with_labels()

        for user_id in users_with_labels:
            if user_id not in labels:
                print(f"Error: User {user_id} has has_labels set to true, but no transportation labels were found.")
                continue

            activities = self.get_user_activities(user_id)
            labels_found = False

            for activity in activities:
                transportation_mode = self.find_matching_label(user_id, activity, labels)
                if transportation_mode:
                    self.update_activity_transportation_mode(activity['activity_id'], transportation_mode)
                    labels_found = True

            if not labels_found:
                print(f"Error: User {user_id} has has_labels set to true, but no matching transportation labels were found.")

        print("Transportation modes updated successfully")

    def get_users_with_labels(self) -> List[str]:
        return [doc['user_id'] for doc in self.db.users.find({"has_labels": True})]

    def get_user_activities(self, user_id: str) -> List[Dict]:
        return list(self.db.activities.find(
            {"user_id": user_id},
            {"activity_id": 1, "start_date_time": 1, "end_date_time": 1}
        ))

    def update_activity_transportation_mode(self, activity_id: int, transportation_mode: str):
        self.db.activities.update_one(
            {"activity_id": activity_id},
            {"$set": {"transportation_mode": transportation_mode}}
        )

    def read_labels(self, dataset_path: str) -> Dict:
        labels = {}
        labeled_ids_path = os.path.join(dataset_path, 'dataset', 'labeled_ids.txt')
        
        with open(labeled_ids_path, 'r') as f:
            labeled_ids = set(f.read().splitlines())
        
        for user_id in labeled_ids:
            labels_file = os.path.join(dataset_path, 'dataset', 'Data', user_id, 'labels.txt')
            if os.path.exists(labels_file):
                with open(labels_file, 'r') as f:
                    user_labels = []
                    for line in f.readlines()[1:]:
                        parts = line.strip().split('\t')
                        if len(parts) == 3:
                            start_time = datetime.datetime.strptime(parts[0], "%Y/%m/%d %H:%M:%S")
                            end_time = datetime.datetime.strptime(parts[1], "%Y/%m/%d %H:%M:%S")
                            mode = parts[2]
                            user_labels.append((start_time, end_time, mode))
                    labels[user_id] = user_labels
        
        return labels

    def find_matching_label(self, user_id: str, activity: Dict, labels: Dict) -> str:
        if user_id not in labels:
            return None
        
        for start_time, end_time, mode in labels[user_id]:
            if (start_time == activity['start_date_time'] and 
                end_time == activity['end_date_time']):
                return mode
        
        return None

    def verify_transportation_modes(self, dataset_path: str):
        labels = self.read_labels(dataset_path)
        users_with_labels = self.get_users_with_labels()
        
        total_activities = 0
        correct_activities = 0
        inconsistent_activities = []

        for user_id in users_with_labels:
            if user_id not in labels:
                print(f"Error: User {user_id} has has_labels set to true, but no labels file was found.")
                continue
            
            activities = self.get_user_activities_with_transportation(user_id)
            
            for activity in activities:
                total_activities += 1
                label_mode = self.find_matching_label(user_id, activity, labels)
                
                if label_mode is not None:
                    if label_mode == activity['transportation_mode']:
                        correct_activities += 1
                    else:
                        inconsistent_activities.append({
                            'user_id': user_id,
                            'activity_id': activity['activity_id'],
                            'db_mode': activity['transportation_mode'],
                            'label_mode': label_mode
                        })

        print(f"Verification complete. {correct_activities} out of {total_activities} activities with labels are correct.")
        
        if inconsistent_activities:
            print("\nInconsistent activities found:")
            for activity in inconsistent_activities:
                print(f"User: {activity['user_id']}, Activity: {activity['activity_id']}, "
                      f"DB Mode: {activity['db_mode']}, Label Mode: {activity['label_mode']}")
        else:
            print("No inconsistencies found.")

    def get_user_activities_with_transportation(self, user_id: str) -> List[Dict]:
        return list(self.db.activities.find(
            {"user_id": user_id},
            {
                "activity_id": 1,
                "start_date_time": 1,
                "end_date_time": 1,
                "transportation_mode": 1
            }
        ))

def main():
    program = None
    try:
        program = ActivityTrackerProgram()
        program.drop_collections()
        program.create_collections()
        
        dataset_path = 'dataset'
        program.populate_user_table(dataset_path)
        program.populate_activities(dataset_path)
        
        program.fetch_data("users")
        program.fetch_data("activities")
        program.show_collections()
        
        program.update_transportation_modes(dataset_path)
        program.verify_transportation_modes(dataset_path)
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()