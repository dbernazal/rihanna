defmodule Rihanna.MetricsTest do
  use ExUnit.Case, async: false
  import TestHelper

  def handle_event(name, _measurements, _metadata, _config) do
    send(self(), List.last(name))
  end

  def setup_assertion_handler(event) do
    :telemetry.attach(
      "rihanna-metric-handler-#{System.unique_integer()}",
      event,
      &handle_event/4,
      nil
    )
  end

  setup do
    {:ok, _pid} =
      start_supervised(%{
        id: Rihanna.Metrics,
        start:
          {Rihanna.Metrics, :start_link,
           [[db: Application.fetch_env!(:rihanna, :postgrex)], [name: Rihanna.Metrics]]}
      })

    :ok
  end

  describe "send_failure_event/1" do
    test "triggers the correct handler for the telemetry event" do
      setup_assertion_handler([:rihanna, :job, :failure])
      Rihanna.Metrics.send_failure_event(%{job_id: 1, count: 5})
      assert_received :failure
    end
  end

  describe "send_success_event/1" do
    test "triggers the correct handler for the telemetry event" do
      setup_assertion_handler([:rihanna, :job, :success])
      Rihanna.Metrics.send_success_event(%{job_id: 1, count: 5})
      assert_received :success
    end
  end

  describe "send_enqueued_event/1" do
    test "triggers the correct handler for the telemetry event" do
      setup_assertion_handler([:rihanna, :job, :enqueued])
      Rihanna.Metrics.send_enqueued_event(%{job_id: 1, count: 5})
      assert_received :enqueued
    end
  end

  describe "send_pending_queue_count/1" do
    test "triggers the correct handler for the telemetry event" do
      setup_assertion_handler([:rihanna, :job, :pending_queue_count])
      Rihanna.Metrics.send_pending_queue_count()
      assert_received :pending_queue_count
    end
  end

  describe "send_dead_queue_count/1" do
    test "triggers the correct handler for the telemetry event" do
      setup_assertion_handler([:rihanna, :job, :dead_queue_count])
      Rihanna.Metrics.send_dead_queue_count()
      assert_received :dead_queue_count
    end
  end

  setup_all :create_jobs_table

  describe "running_queue_count/1" do
    setup do
      {:ok, _pid} =
        start_supervised(%{
          id: Rihanna.JobDispatcher,
          start:
            {Rihanna.JobDispatcher, :start_link,
             [[db: Application.fetch_env!(:rihanna, :postgrex)], [name: Rihanna.JobDispatcher]]}
        })

      :ok
    end

    test "triggers the correct handler for the telemetry event" do
      setup_assertion_handler([:rihanna, :job, :running_queue_count])
      Rihanna.Metrics.send_running_queue_count()
      assert_received :running_queue_count
    end
  end

  describe "pending_queue_count/1" do
    setup do
      {:ok, _pid} =
        start_supervised(%{
          id: Rihanna.JobDispatcher,
          start:
            {Rihanna.JobDispatcher, :start_link,
             [[db: Application.fetch_env!(:rihanna, :postgrex)], [name: Rihanna.JobDispatcher]]}
        })

      :ok
    end

    test "sends the correct genserver message for 1 pending message", %{pg: pg} do
      insert_job(pg, :scheduled_at)
      assert Rihanna.Metrics.pending_queue_count() == 1
    end

    test "sends the correct genserver message for 1 pending message and other messages", %{pg: pg} do
      insert_job(pg, :scheduled_at)
      insert_job(pg, :failed)
      insert_job(pg, :ready_to_run)

      assert {:noreply, _state} =
               Rihanna.JobDispatcher.handle_info(:poll, %{working: %{}, pg: pg})

      assert Rihanna.Metrics.pending_queue_count() == 2
    end
  end

  describe "dead_queue_count/1" do
    test "triggers the correct handler for the telemetry event" do
    end
  end

  describe "handle_call/1" do
    test "returns the correct dead queue count" do
    end

    test "returns the correct pending queue count" do
    end
  end
end
