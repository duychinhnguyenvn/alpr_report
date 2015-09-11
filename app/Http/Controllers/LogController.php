<?php

namespace App\Http\Controllers;
use DB;
use DateTime;
use DateTimeZone;
use Illuminate\Http\Request;	
use App\Http\Controllers\Controller;
use App\Process;
use App\Jobs\TransformLog;
use App\Jobs\DailyAggregationJob;
use App\Jobs\MonthlyAggregationJob;

class LogController extends Controller
{
    /**
     * Show the profile for the given user.
     *
     * @param  int  $id
     * @return Response
     */
    public function log(Request $request)
    {
        $plate =  $request->input('plate');
		$area = $request->input('area');
		$location = $request->input('location');
		$file=null;
		$fileName='';
		$timezone = config('app.timezone');
		$date = new DateTime();
		$date->setTimezone(new DateTimeZone($timezone));
		foreach($request->file() as $file_) {
			//process each file
			$file=$file_;
		}
		if($file!=null){
			$fileName=$date;
			$fileName=$location."-".$plate."-".$fileName->format('YmdHis').".jpg";
			$destinationPath=storage_path().'/uploads/';
			$file->move($destinationPath,$fileName);	
		}
		
		DB::insert('insert into raw_logs (plate,location,image,area,created_at) values (?,?,?,?,?)', [$plate, $location,$fileName,$area,$date]);
		
		return 'Hello CHinh';
    }
	/**
	* Insert process transform 
	*/
	public function fn_refresh_insert_tranform_log_process(){
		$process_error=Process::where('status', 'PS')->orWhere('status', 'WT')->orWhere('status', 'TR')->orWhere('status','LIKE', 'LR%')->where('process_config_id',1)->first();
		if($process_error!=null){
			return $process_error;
		}		
		$last_process_success=Process::where('status', 'SU')->where('process_config_id',1)->orderBy('id','desc')->first();		
		$load_min_max_log_id_result=DB::select('SELECT MIN(id) as min,MAX(id) as max FROM raw_logs');
		$start_log_id=$load_min_max_log_id_result[0]->min;
		$end_log_id=$load_min_max_log_id_result[0]->max;
		
		if($end_log_id==0){
			return 'logs empty to process';
		}
		
		if($last_process_success!=null){
			if($last_process_success->end_log_id == $end_log_id){
				return 'No new logs to process';
			}
			$start_log_id=$last_process_success->end_log_id+1;
		}
		$timezone = config('app.timezone');
		$date = new DateTime();
		$date->setTimezone(new DateTimeZone($timezone));
		
		$process=new Process;
			$process->start_log_id=$start_log_id;
			$process->end_log_id=$end_log_id;
			$process->status='WT';
			$process->process_config_id=1;
			$process->created_at=$date;
			$process->updated_at=$date;
			$process->save();
			
			$job = (new TransformLog($process))->onQueue('etl')->delay(60);
			$this->dispatch($job);
	}
	/**
	* Function generate daily data
	*/
	public function fn_generate_daily_agg($start_date=null,$end_date=null){	
		
		$process_running=Process::where('status', 'PS')->orWhere('status', 'WT')->where('process_config_id',2)->first();
		if($process_running!=null){
			return $process_running;
		}
		$last_process_su=Process::where('status', 'SU')->where('process_config_id',2)->orderBy('id','desc')->first();
		
		
		$load_min_max_log_id_result;
		if($start_date!=null && $end_date!=null){
			$load_min_max_log_id_result=DB::select('SELECT MIN(log_id) as min,MAX(log_id) as max, MIN(a.date_key) as min_date_key,MAX(a.date_key) as max_date_key
					FROM fct_logs a
					INNER JOIN dim_date b ON a.date_key=b.date_key AND b.full_date BETWEEN ? AND ?',[$start_date,$end_date]);
		}else{
			$load_min_max_log_id_result=DB::select('SELECT MIN(log_id) as min,MAX(log_id) as max, MIN(a.date_key) as min_date_key,MAX(a.date_key) as max_date_key
					FROM fct_logs a
					INNER JOIN dim_date b ON a.date_key=b.date_key AND b.full_date BETWEEN DATE(now())-2 AND DATE(now())');
		}
		
		$start_log_id=$load_min_max_log_id_result[0]->min;
		$end_log_id=$load_min_max_log_id_result[0]->max;
		$start_date_key=$load_min_max_log_id_result[0]->min_date_key;
		$end_date_key=$load_min_max_log_id_result[0]->max_date_key;
		
		if($last_process_su!=null && $last_process_su->start_log_id==$start_log_id && $last_process_su->end_log_id==$end_log_id  && 
			$last_process_su->start_date_key==$start_date_key && $last_process_su->end_date_key==$end_date_key){
			return 'No new logs to process'.$last_process_su;
		}
		
		
		$timezone = config('app.timezone');
		$date = new DateTime();
		$date->setTimezone(new DateTimeZone($timezone));
		
		$process=new Process;
			$process->start_log_id=$start_log_id;
			$process->end_log_id=$end_log_id;
			$process->start_date_key=$start_date_key;
			$process->end_date_key=$end_date_key;
			$process->status='WT';
			$process->process_config_id=2;
			$process->created_at=$date;
			$process->updated_at=$date;
			$process->save();
			
			$job = (new DailyAggregationJob($process))->onQueue('agg')->delay(60);
			$this->dispatch($job);
			
	}
	/**
	* Function generate daily data
	*/
	public function fn_generate_monthly_agg(){	
		$process_running=Process::where('status', 'PS')->orWhere('status', 'WT')->where('process_config_id',3)->first();
		if($process_running!=null){
			return $process_running;
		}
		$last_process_su=Process::where('status', 'SU')->where('process_config_id',3)->orderBy('id','desc')->first();
		
		$load_min_max_log_id_result=DB::select('SELECT MIN(log_id) as min,MAX(log_id) as max, MIN(a.date_key) as min_date_key,MAX(a.date_key) as max_date_key
					FROM fct_logs a
					INNER JOIN dim_date b ON a.date_key=b.date_key AND b.full_date BETWEEN DATE_FORMAT(NOW() - INTERVAL 1 MONTH, \'%Y-%m-01 00:00:00\') AND DATE_FORMAT(LAST_DAY(NOW()), \'%Y-%m-%d 23:59:59\')');
		$start_log_id=$load_min_max_log_id_result[0]->min;
		$end_log_id=$load_min_max_log_id_result[0]->max;
		$start_date_key=$load_min_max_log_id_result[0]->min_date_key;
		$end_date_key=$load_min_max_log_id_result[0]->max_date_key;
		
		if($last_process_su!=null && $last_process_su->start_log_id==$start_log_id && $last_process_su->end_log_id==$end_log_id  && 
			$last_process_su->start_date_key==$start_date_key && $last_process_su->end_date_key==$end_date_key){
			return 'No new logs to process'.$last_process_su;
		}
		
		
		$timezone = config('app.timezone');
		$date = new DateTime();
		$date->setTimezone(new DateTimeZone($timezone));
		
		$process=new Process;
			$process->start_log_id=$start_log_id;
			$process->end_log_id=$end_log_id;
			$process->start_date_key=$start_date_key;
			$process->end_date_key=$end_date_key;
			$process->status='WT';
			$process->process_config_id=3;
			$process->created_at=$date;
			$process->updated_at=$date;
			$process->save();
			
			$job = (new MonthlyAggregationJob($process))->onQueue('agg')->delay(60);
			$this->dispatch($job);
			
	}
}