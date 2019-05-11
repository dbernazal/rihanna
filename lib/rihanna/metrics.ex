defmodule Rihanna.Metrics do
  use GenServer

  @moduledoc false

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
        {__MODULE__, :dispatch_pending_queue_count, []},
        {__MODULE__, :dispatch_dead_queue_count, []},
        {__MODULE__, :dispatch_running_queue_count, []}
      ],
      period: 10_000
    )

    state = %{pg: pg}

    {:ok, state}
  end

  def send_failure_event(%{job_id: job_id, count: count}),
    do: :telemetry.execute([:rihanna, :job, :failure], %{count: count}, %{job_id: job_id})

  def send_success_event(%{job_id: job_id, count: count}),
    do: :telemetry.execute([:rihanna, :job, :success], %{count: count}, %{job_id: job_id})

  def send_enqueued_event(%{job_id: job_id, count: count}),
    do: :telemetry.execute([:rihanna, :job, :enqueued], %{count: count}, %{job_id: job_id})

  def dispatch_pending_queue_count(),
    do:
      :telemetry.execute([:rihanna, :job, :pending_queue_count], %{count: pending_queue_count()})

  def dispatch_dead_queue_count(),
    do: :telemetry.execute([:rihanna, :job, :dead_queue_count], %{count: dead_queue_count()})

  def dispatch_running_queue_count(),
    do:
      :telemetry.execute([:rihanna, :job, :running_queue_count], %{count: running_queue_count()})

  def pending_queue_count(), do: GenServer.call(__MODULE__, :pending_queue_count)

  def dead_queue_count(), do: GenServer.call(__MODULE__, :dead_queue_count)

  defdelegate running_queue_count(), to: Rihanna.JobDispatcher

  def handle_call(:dead_queue_count, _from, %{pg: pg} = state),
    do: {:reply, Rihanna.Job.dead_queue_count(pg), state}

  def handle_call(:pending_queue_count, _from, %{pg: pg} = state),
    do: {:reply, Rihanna.Job.pending_queue_count(pg), state}
end
