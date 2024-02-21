package cn.itcast.hotel;

import cn.itcast.hotel.service.IHotelService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@SpringBootTest
class HotelDemoApplicationTests {

    @Resource
    private IHotelService hotelService;
    @Test
    void contextLoads() {
    }

    @Test
    void testFilter(){
//        Map<String, List<String>> filter = hotelService.filter(params);
//        System.out.println("filter = " + filter);
    }

}
