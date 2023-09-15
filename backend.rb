# This code groups events which occur within 10 minutes of each other, similar to a session
# It keeps track of each "session" duration by calculating the difference between the current and previous events create date
# If the event is the first one or the duration of the previous event exceeds 10 minutes it resets

# distinct_groups stores the aforementioned duration data in a hash that can be looked up by user_name

# This can be used to track user engagement or load on the system

BATCH_SIZE = 100

start_range = Date.today.beginning_of_month
end_range = Date.today.end_of_month

employers = []
partners = []
controller_resources = []

distinct_groups = {}

# includes avoids n+1 query
users = User.includes(:user_events).where(role: role)

# find_each is more memory efficient for large datasets
users.find_each(batch_size: BATCH_SIZE) do |users|
  time = 0
  last_time = nil
  times = []
  user_name = nil

  # find_each is more memory efficient for large datasets
  # filtering and ordering is more efficient at the database level as opposed to in memory for large datasets
  users.user_events.where(created_at: start_range..end_range).order(:created_at).find_each(batch_size: BATCH_SIZE) do |event|
    user_name ||= event.user_name
    last_known_session = event.last_known_session

    if last_known_session.present?
      employers << last_known_session["employer"]
      partners << last_known_session["partner"]
    end

    # access data without raising exceptions
    controller_resources << event.data.dig("params", "controller")

    # added parenthesis for improved readability
    if last_time.nil? || (last_time + 10.minutes < event.created_at)
      times << time
      time = 0
    else
      time += event.created_at - last_time
    end

    last_time = event.created_at
  end

  times << time
  # sum is more efficient than reduce(:+)
  distinct_groups[user_name] = times.sum
end
