defmodule Rihanna.Metrics do
  use GenServer

  @moduledoc false

  @job_event_prefix [:rihanna, :job]
  @success_event @job_event_prefix ++ [:success]
  @failure_event @job_event_prefix ++ [:failure]
  @enqueued_event @job_event_prefix ++ [:enqueued]
  @pending_queue_count_event @job_event_prefix ++ [:pending_queue_count]
  @running_queue_count_event @job_event_prefix ++ [:running_queue_count]
  @dead_queue_count_event @job_event_prefix ++ [:dead_queue_count]

  def start_link(config, opts) do
    GenServer.start_link(__MODULE__, config, opts)
  end

  @doc false
  def init(config) do
    db = Keyword.get(config, :db)

    # NOTE: These are linked because it is important that the pg session is also
    # killed if the JobDispatcher dies since otherwise we may leave dangling
    # locks in the zombie pg process
    {:ok, pg} = Postgrex.start_link(db)

    Telemetry.Poller.start_link(
      measurements: [
        {__MODULE__, :send_pending_queue_count, []},
        {__MODULE__, :send_dead_queue_count, []},
        {__MODULE__, :send_running_queue_count, []}
      ],
      period: 10_000
    )

    {:ok, %{pg: pg}}
  end

  def send_failure_event(%{job_id: job_id, count: count}),
    do: :telemetry.execute(@failure_event, %{count: count}, %{job_id: job_id})

  def send_success_event(%{job_id: job_id, count: count}),
    do: :telemetry.execute(@success_event, %{count: count}, %{job_id: job_id})

  def send_enqueued_event(%{job_id: job_id, count: count}),
    do: :telemetry.execute(@enqueued_event, %{count: count}, %{job_id: job_id})

  def send_pending_queue_count(),
    do: :telemetry.execute(@pending_queue_count_event, %{count: pending_queue_count()})

  def send_dead_queue_count(),
    do: :telemetry.execute(@dead_queue_count_event, %{count: dead_queue_count()})

  def send_running_queue_count(),
    do: :telemetry.execute(@running_queue_count_event, %{count: running_queue_count()})

  defdelegate running_queue_count(), to: Rihanna.JobDispatcher

  def pending_queue_count(), do: GenServer.call(__MODULE__, :pending_queue_count)

  def dead_queue_count(), do: GenServer.call(__MODULE__, :dead_queue_count)

  def handle_call(:dead_queue_count, _from, %{pg: pg} = state),
    do: {:reply, Rihanna.Job.dead_queue_count(pg), state}

  def handle_call(:pending_queue_count, _from, %{pg: pg} = state),
    do: {:reply, Rihanna.Job.pending_queue_count(pg), state}
end
