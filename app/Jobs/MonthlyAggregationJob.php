<?php

namespace App\Jobs;

use DB;
use Exception;
use DateTime;
use DateTimeZone;
use App\Jobs\Job;
use App\Process;
use Illuminate\Queue\SerializesModels;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Contracts\Bus\SelfHandling;
use Illuminate\Contracts\Queue\ShouldQueue;

class MonthlyAggregationJob extends Job implements SelfHandling, ShouldQueue
{
    use InteractsWithQueue, SerializesModels;
	protected $process;
	// Update process status
	private function updateProcessStatus($status){
		$timezone = config('app.timezone');
		$date = new DateTime();
		$date->setTimezone(new DateTimeZone($timezone));
		
		$this->process->status=$status;
		$this->process->updated_at=$date;
		$this->process->save();
	}
    /**
     * Create a new job instance.
     *
     * @return void
     */
    public function __construct(Process $process)
    {
        //
		$this->process=$process;
    }

    /**
     * Execute the job.
     *
     * @return void
     */
    public function handle()
    {
        //
		$this->updateProcessStatus('PS');
		$timezone = config('app.timezone');
		$date = new DateTime();
		$date->setTimezone(new DateTimeZone($timezone));
		DB::statement('INSERT INTO agg_monthly (calendar_year_month,calendar_year,month_of_year,plate_key,plate_number,location_key,counts,dt_last_changed,process_id)  
						SELECT b.calendar_year_month,b.calendar_year,b.month_of_year,a.plate_key,c.plate_number,a.location_key,COUNT(*) as counts, ?, ?
						FROM fct_logs a
						LEFT JOIN dim_date b ON a.date_key=b.date_key
						LEFT JOIN dim_plate c ON a.plate_key=c.plate_key
						WHERE a.is_active = ? AND a.date_key BETWEEN ? AND ?  						
						GROUP BY b.calendar_year_month,b.calendar_year,b.month_of_year,c.plate_number,a.plate_key,a.location_key',
						[$date,$this->process->id,true,$this->process->start_date_key,$this->process->end_date_key]);
		$list_months=DB::select('SELECT calendar_year_month FROM agg_monthly WHERE process_id=?',[$this->process->id]);
		foreach($list_months as $month){
			DB::statement('UPDATE agg_monthly SET is_active = ? WHERE process_id < ? AND calendar_year_month = ?',[false,$this->process->id,$month->calendar_year_month]);
		}		
		DB::statement('UPDATE agg_daily SET is_active = ? WHERE process_id = ?',[true,$this->process->id]);
		
		foreach($list_months as $month){
			DB::statement('DELETE FROM agg_monthly WHERE is_active = ? AND process_id < ? AND calendar_year_month = ?',[false,$this->process->id,$month->calendar_year_month]);
		}
		
		$this->updateProcessStatus('SU');
    }
	/**
	 * Handle a job failure.
	 *
	 * @return void
	 */
    public function failed()
    {
        // Called when the job is failing...		
		DB::statement('DELETE FROM agg_daily WHERE process_id = ? ',[$this->process->id]);
		$this->updateProcessStatus('ER');
    }
}
