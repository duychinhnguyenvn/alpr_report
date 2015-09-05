<?php

namespace App\Http\Controllers;
use DB;
use App\Http\Controllers\Controller;

class LogController extends Controller
{
    /**
     * Show the profile for the given user.
     *
     * @param  int  $id
     * @return Response
     */
    public function log()
    {
        string plate = Input::get("plate");
		//string[] xywh = Request.QueryString["area"].ToString().Split(',');
		//string location = Request.QueryString["location"];
		DB::insert('insert into logs (plate, image,capture_time) values (?, ?,?)', [plate, 'Dayle','2015-01-02 00:00:00']);
		
		//Bitmap bmp = new Bitmap(Request.Files[0].InputStream);

		//integrate plate, area, location and plate image with your systems
				
		//bmp.Dispose();
		return 'Hello CHinh';
    }
}