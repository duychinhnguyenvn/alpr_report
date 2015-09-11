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

class TransformLog extends Job implements SelfHandling, ShouldQueue
{
    use InteractsWithQueue, SerializesModels;
	protected $process;
	
	// Script load log to staging
	private function stagingLoad(){
		DB::statement('INSERT INTO stg_logs (id,created_at,plate,location,image,area)  
						SELECT id,created_at,plate,location,image,area  
						FROM raw_logs 
						WHERE id BETWEEN ? AND ?',
						[$this->process->start_log_id,$this->process->end_log_id]);
	}
	// Script trasnform Date dim
	private function transformDateDim(){
		DB::statement('UPDATE stg_logs a
						LEFT JOIN dim_date b ON DATE(a.created_at) = b.full_date
						SET a.date_key=IF(b.date_key IS NULL,-100,b.date_key)
					');
	}
	// Script transform Plate dim
	private function transformPlateDim(){
		DB::statement('UPDATE stg_logs a
						LEFT JOIN dim_plate b ON a.plate = b.plate_number
						SET a.plate_key=IF(b.plate_key IS NULL,-100,b.plate_key)
					');
	}
	
	// Script transform Location dim
	private function transformLocationDim(){
		DB::statement('UPDATE stg_logs a
						LEFT JOIN dim_location b ON a.location = b.location_id
						SET a.location_key=IF(b.location_key IS NULL,-100,b.location_key)
					');
	}
	
	// Script load staging to fact
	private function factLoad(){
		DB::statement('INSERT INTO fct_logs (captured_at,log_id,image,area,date_key,plate_key,location_key,process_id)  
						SELECT created_at,id,image,area,date_key,plate_key,location_key, ?  FROM stg_logs
					',[$this->process->id]);
	}
	// Promote transform process
	private function promote(){
		DB::statement('UPDATE fct_logs SET is_active = ? WHERE process_id = ?
					',[true,$this->process->id]);	
		DB::statement('TRUNCATE TABLE stg_logs');					
	}	
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
		
		// Verify staging space
		$staging_log_row_count=DB::select('SELECT COUNT(*) as count FROM stg_logs');
		if($staging_log_row_count[0]->count>0){
			throw new Exception('Please Truncate stg_logs table');
		}
		
		// Copy raw log to staging		
		$this->stagingLoad();
		$this->updateProcessStatus('TR');	
		
		// Transform date dim
		$this->transformDateDim();
		$this->updateProcessStatus('TR1');
		
		// Transform plate dim
		$this->transformPlateDim();
			// Update plate dim
			$plates=DB::select('select plate from stg_logs WHERE plate_key = ? GROUP BY plate',[-100]);
			$reTransformPlate=false;
			foreach ($plates as $plate) {
				$plates=DB::table('dim_plate')->insert(['plate_number' => $plate->plate]);
				$reTransformPlate=true;
			}
			if($reTransformPlate){
				$this->transformPlateDim();
			}
		$this->updateProcessStatus('TR2');
		
		// Transform location dim
		$this->transformLocationDim();
			// Update location dim
			$locations=DB::select('select location from stg_logs WHERE location_key = ? GROUP BY location',[-100]);
			$reTransformLocation=false;
			foreach ($locations as $location) {
				$plates=DB::table('dim_location')->insert(['location_id' => $location->location]);
				$reTransformLocation=true;
			}
			if($reTransformLocation){
				$this->transformLocationDim();
			}
		$this->updateProcessStatus('LR');
		
		// fact load		
		$this->factLoad();	
		// backup logs
		$this->promote();
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
		DB::statement('DELETE FROM fct_logs WHERE process_id = ? ',[$this->process->id]);
		
		$timezone = config('app.timezone');
		$date = new DateTime();
		$date->setTimezone(new DateTimeZone($timezone));
		
		$this->process->status='ER';
		$this->process->updated_at=$date;
		$this->process->save();
    }
}
