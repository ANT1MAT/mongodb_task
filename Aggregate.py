#Агрегационный запрос

db.collection.aggregate([
  {
    "$unwind": "$sessions"
  },
  {
    "$unwind": "$sessions.actions"
  },
  {
    "$group": {
      "_id": {
        "number": "$number",
        "type": "$sessions.actions.type"
      },
      "count": {
        '$sum': 1
      },
      "last": {
        "$last": "$sessions.actions.created_at"
      }
    }
  },
  {
    "$group": {
      "_id": "$_id.number",
      "actions": {
        "$push": {
          "type": "$_id.type",
          "count": "$count",
          "last": "$last"
        }
      }
    }
  },
  {
    "$project": {
      "_id": 0,
      "number": "$_id",
      "actions": "$actions"
    }
  }
])