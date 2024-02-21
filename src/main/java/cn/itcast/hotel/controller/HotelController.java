package cn.itcast.hotel.controller;

import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/hotel")
public class HotelController {
    @Resource
    private IHotelService hotelService;
    @PostMapping("/list")
    public PageResult<HotelDoc> list(@RequestBody RequestParams params){
        return hotelService.search(params);
    }

    @PostMapping("/filters")
    public Map<String, List<String>> filters(@RequestBody RequestParams params){
        return hotelService.filter(params);
    }

    @GetMapping("/suggestion")
    public List<String> getSuggestions(@RequestParam String key){
        return hotelService.getSuggestions(key);
    }
}
